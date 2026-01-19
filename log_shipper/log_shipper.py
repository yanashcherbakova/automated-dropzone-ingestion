import os
import time
import glob
from dotenv import load_dotenv

load_dotenv()

LOGS_DIR = os.getenv("LOGS_DIR")

def ship_ratated_logs(LOGS_DIR):
    pattern = os.path.join(LOGS_DIR, "dropzone.log.*")

    while True:
        time.sleep(60)

        for path in sorted(glob.glob(pattern)):
            try:
                size1 = os.path.getsize(path)
                time.sleep(1)
                size2 = os.path.getsize(path)
                if size1 != size2:
                    continue
            except FileNotFoundError:
                continue

            fname = os.path.basename(path)
            key = f"logs/{fname}"

            try:
                s3.upload_file(path, bucket, key)
                os.remove(path)
            except Exception:
                pass
    