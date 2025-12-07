import os
import re
import boto3
import psycopg2
from datetime import date, timedelta

# ---------- Environment Variables ----------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS")
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
    region_name=AWS_REGION,
)


def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )


# ------------------------------------------------------------
#   Time normalization helper (fixes 1:27:48 / 01:27:48 PM)
# ------------------------------------------------------------
def normalize_time(t: str):
    """Return HH:MM:SS (24-hour) or None if it can't be parsed."""
    t = t.strip()
    t = t.replace("AM", "").replace("PM", "").strip()

    parts = t.split(":")
    if len(parts) != 3:
        return None

    try:
        hh = f"{int(parts[0]):02d}"
        mm = f"{int(parts[1]):02d}"
        ss = f"{int(parts[2]):02d}"
        return f"{hh}:{mm}:{ss}"
    except Exception:
        return None


# ------------------------------------------------------------
#   Fetch last processed bill (for tail-seek)
# ------------------------------------------------------------
def get_last_processed_bill(cursor):
    cursor.execute(
        """
        SELECT bill, log_dt, log_time
          FROM loader_log
      ORDER BY log_dt DESC, log_time DESC
         LIMIT 1;
        """
    )
    row = cursor.fetchone()
    if row:
        bill, log_dt, log_time = row
        print(f"🧭 Last processed bill: {bill} at {log_dt} {log_time}")
        return bill
    else:
        print("🧭 No previous bills, processing full file.")
        return None


# ------------------------------------------------------------
#   Core parser: scan file and pair RTC True ↔ CallRTCControllerByCode
# ------------------------------------------------------------
def parse_loader_file(lines, last_bill=None):
    """
    Returns list of tuples:
        (bill, washify_rec, log_dt, log_time)
    where:
      - bill        = invoice id from 'RTC True' line
      - washify_rec = invoice id from 'CallRTCControllerByCode' line
      - log_dt/time come from the RTC True line
    """

    # If we have a last processed bill, fast-forward to AFTER that RTC True line
    start_index = 0
    if last_bill:
        pattern = f"RTC True, Invoice Id {last_bill}"
        for idx, line in enumerate(lines):
            if pattern in line:
                start_index = idx + 1
                print(f"⏩ Skipping up to line {idx} (last bill {last_bill})")
                break

    results = []
    current_bill = None
    current_dt = None
    current_time = None

    for idx in range(start_index, len(lines)):
        line = lines[idx].strip()
        if not line:
            continue

        # Timestamp is the part before the first comma
        ts_match = re.match(r"^([^,]+)", line)
        if not ts_match:
            continue

        ts_raw = ts_match.group(1).strip()
        parts = ts_raw.split(" ", 1)
        if len(parts) != 2:
            continue

        dt_str, time_raw = parts
        time_norm = normalize_time(time_raw)
        if not time_norm:
            continue

        # 1️⃣ RTC True line → start of a pair; we grab bill + timestamp
        if "RTC True" in line and "Invoice Id" in line:
            m_id = re.search(r"Invoice Id (\d+)", line)
            if not m_id:
                continue

            current_bill = int(m_id.group(1))
            current_dt = dt_str
            current_time = time_norm
            # (we wait for the next CallRTCControllerByCode line)
            continue

        # 2️⃣ Controller line → completes the pair
        if (
            "CallRTCControllerByCode" in line
            and "Invoice Id" in line
            and current_bill is not None
        ):
            m_id = re.search(r"Invoice Id (\d+)", line)
            if not m_id:
                continue

            washify_rec = int(m_id.group(1))
            results.append((current_bill, washify_rec, current_dt, current_time))

            print(
                f"🔗 Paired RTC True bill={current_bill} "
                f"with controller Invoice={washify_rec} at {current_dt} {current_time}"
            )

            # reset for next pair
            current_bill = None
            current_dt = None
            current_time = None

    return results


# ------------------------------------------------------------
#   Process a single yyyy-mm-dd folder under loader1/
# ------------------------------------------------------------
def process_folder(conn, cursor, folder):
    prefix = f"loader1/{folder}/"
    print(f"🔍 Checking folder: {prefix}")

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in response:
        print(f"No files in {prefix}")
        return

    last_bill = get_last_processed_bill(cursor)

    # Go through each text file in that date folder
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.lower().endswith(".txt"):
            continue

        print(f"📄 Detected file: {key}")
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode(
            "utf-8", errors="ignore"
        )
        lines = [l for l in body.splitlines() if l.strip()]

        pairs = parse_loader_file(lines, last_bill=last_bill)
        if not pairs:
            print(f"⚠️ No pairs found in {key}")
            continue

        for bill, washify_rec, log_dt, log_time in pairs:
            prep_end_ts = f"{log_dt} {log_time}"

            # 1) Insert into loader_log if new
            cursor.execute("SELECT 1 FROM loader_log WHERE bill = %s", (bill,))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(
                    """
                    INSERT INTO loader_log (bill, washify_rec, log_dt, log_time)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (bill, washify_rec, log_dt, log_time),
                )
                conn.commit()
                print(f"🆕 Inserted loader_log bill={bill}, washify_rec={washify_rec}")
            else:
                print(f"↻ loader_log bill={bill} already exists")

            # 2) Update SUPER
            try:
                cursor.execute(
                    """
                    UPDATE super
                       SET status = 3,
                           prep_end = %s,
                           status_desc = 'Wash'
                     WHERE bill = %s
                       AND created_on = %s
                       AND location = 'FRA'
                       AND (status IS NULL OR status < 3)
                    """,
                    (prep_end_ts, bill, log_dt),
                )
                if cursor.rowcount > 0:
                    print(f"🧾 SUPER updated for bill={bill}")
                conn.commit()
            except Exception as e:
                print(f"⚠️ SUPER update failed for bill={bill}: {e}")
                conn.rollback()

            # 3) Update TUNNEL
            try:
                cursor.execute(
                    """
                    UPDATE tunnel
                       SET load = TRUE,
                           load_time = %s
                     WHERE bill = %s
                       AND created_on = %s
                       AND location = 'FRA'
                    """,
                    (prep_end_ts, bill, log_dt),
                )
                if cursor.rowcount > 0:
                    print(f"🚗 TUNNEL updated for bill={bill}")
                conn.commit()
            except Exception as e:
                print(f"⚠️ TUNNEL update failed for bill={bill}: {e}")
                conn.rollback()

        # OPTIONAL: delete file once everything has been applied
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            print(f"🧹 Deleted S3 file: {key}")
        except Exception as e:
            print(f"⚠️ Failed to delete file {key}: {e}")


# ------------------------------------------------------------
#   MAIN RUNNER
# ------------------------------------------------------------
def process_files():
    conn = connect_db()
    cursor = conn.cursor()

    today_folder = date.today().strftime("%Y-%m-%d")
    yesterday_folder = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    for folder in [today_folder, yesterday_folder]:
        process_folder(conn, cursor, folder)

    # Heartbeat
    try:
        cursor.execute(
            """
            INSERT INTO heartbeat (source, created_on, created_at)
            VALUES (%s, CURRENT_DATE, CURRENT_TIME)
            """,
            ("Loader2Safari",),
        )
        conn.commit()
        print("💓 Heartbeat logged: Loader2Safari")
    except Exception as e:
        print(f"⚠️ Heartbeat logging failed: {e}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    print("🚀 Loader2Safari single-run mode started...")
    try:
        process_files()
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
