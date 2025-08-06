import os
import time
import psycopg2

# ─── Wait for Postgres to be ready ───────────────────────────────────────────────
db_host = os.getenv('DB_HOST', 'db')
db_port = int(os.getenv('DB_PORT', 5432))
db_name = os.getenv('DB_NAME', 'flask_db')
db_user = os.getenv('DB_USER', 'flask_user')
db_pass = os.getenv('DB_PASSWORD', 'flask_password')

print(f"Waiting for database {db_user}@{db_host}:{db_port}/{db_name}…")
while True:
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass
        )
        conn.close()
        print("✅ Database is up!")
        break
    except psycopg2.OperationalError:
        print("⏳ Still waiting for DB…")
        time.sleep(2)
# ────────────────────────────────────────────────────────────────────────────────

from app import create_app, db

# Try to import process_existing_submissions, fallback to process_submission_file, or skip if not present
process_existing_submissions = None

try:
    from app.etl import process_existing_submissions
except ImportError:
    try:
        from app.etl import process_submission_file
        process_existing_submissions = None  # Not used here, just fallback
        print("⚠️  No 'process_existing_submissions' found in etl.py, skipping this step.")
    except ImportError:
        print("⚠️  No ETL function found in etl.py at all.")

app = create_app()

with app.app_context():
    db.create_all()
    # Only call process_existing_submissions if it exists
    if process_existing_submissions is not None:
        process_existing_submissions()

if __name__ == "__main__":
    # Listen on 5001 so that Docker’s 5001:5000 mapping still works
    app.run(host="0.0.0.0", port=5001)
