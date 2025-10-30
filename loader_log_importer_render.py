import os
import boto3
import psycopg2
import re
import time
from datetime import date

# ---------- Environment Variables ----------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "safari-franklin-data"

# ---------- AWS & DB Setup ----------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# ---------- Process Function ----------
def process_files():
    conn = connect_db()
    cursor = conn.cursor()
    today_folder = date.today().strftime("%Y-%m-%d")
    prefix = f"loader1/{today_folder}/"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in response:
        cursor.close()
        conn.close()
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".txt"):
            continue

        print(f"📄 Detected file: {key}")
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8", errors="ignore")
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        inserted_count = 0

        for i in range(0, len(lines), 4):
            try:
                line1, line2, line4 = lines[i], lines[i + 1], lines[i + 3]

                # Extract timestamp
                ts_match = re.match(r"^([^,]+)", line1)
                timestamp = ts_match.group(1).strip() if ts_match else ""
                date_part, time_part = timestamp.split(" ", 1)
                time_part = time_part.replace("AM", "").replace("PM", "").strip()

                bill = int(re.search(r"Invoice Id (\d+)", line2).group(1))
                washify_rec = int(re.search(r"Invoice Id (\d+)", line4).group(1))

                # ---- Insert if new ----
                cursor.execute("SELECT 1 FROM loader_log WHERE bill = %s", (bill,))
                exists = cursor.fetchone()
                if not exists:
                    cursor.execute("""
                        INSERT INTO loader_log (bill, washify_rec, log_dt, log_time)
                        VALUES (%s, %s, %s, %s)
                    """, (bill, washify_rec, date_part, time_part))
                    conn.commit()
                    inserted_count += 1
                    print(f"✅ Inserted bill={bill}")
                else:
                    print(f"↻ Bill {bill} already exists, skipping insert but continuing updates")

                # ---- Step #1: Update SUPER ----
                try:
                    cursor.execute("""
                        UPDATE super
                           SET status = 3,
                               prep_end = %s,
                               status_desc = 'Wash'
                         WHERE bill = %s
                           AND created_on = %s
                           AND location = 'FRA'
                           AND (status IS NULL OR status < 3)
                    """, (time_part, bill, date_part))
                    if cursor.rowcount > 0:
                        print(f"🧾 SUPER updated for bill={bill}")
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ SUPER update failed for bill={bill}: {e}")
                    conn.rollback()

                # ---- Step #2: Update TUNNEL ----
                try:
                    cursor.execute("""
                        UPDATE tunnel
                           SET load = TRUE,
                               load_time = %s
                         WHERE bill = %s
                           AND created_on = %s
                           AND location = 'FRA'
                    """, (time_part, bill, date_part))
                    if cursor.rowcount > 0:
                        print(f"🚗 TUNNEL updated for bill={bill}")
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ TUNNEL update failed for bill={bill}: {e}")
                    conn.rollback()

            except Exception as e:
                print(f"❌ Error parsing block {i}: {e}")

        # ---- Move file to Archive ----
        archive_key = f"loader1/Archive/{key.split('/')[-1]}"
        try:
            s3.copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": key}, Key=archive_key)
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            print(f"📦 Moved file to Archive: {archive_key}")
        except Exception as e:
            print(f"⚠️ Failed to move {key} to Archive: {e}")

        print(f"✅ File processed: {key}, {inserted_count} new records.\n")

    # ---- Heartbeat ----
    try:
        cursor.execute("""
            INSERT INTO heartbeat (source, created_on, created_at)
            VALUES (%s, CURRENT_DATE, CURRENT_TIME)
        """, ("Loader2Safari",))
        conn.commit()
        print("💓 Heartbeat logged: Loader2Safari")
    except Exception as e:
        print(f"⚠️ Heartbeat logging failed: {e}")

    cursor.close()
    conn.close()

# ---------- Continuous Monitor ----------
if __name__ == "__main__":
    print("🚀 Loader2Safari Real-Time Monitor started...")
    while True:
        try:
            process_files()
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}")
        time.sleep(5)  # check every 5 seconds
