import os
import time
import glob
from dotenv import load_dotenv
from aws.s3_utils import upload_to_s3

load_dotenv()

def ship_ratated_logs(s3, bucket, logger_uploader):
    LOGS_DIR = os.getenv("LOGS_DIR")
    FAILED_LOGS = os.getenv("FAILED_LOGS")
    s3_prefix = "logs"

    os.makedirs(FAILED_LOGS, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)

    
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

            upload_to_s3(
                s3=s3,
                file_path=path,
                logger_uploader=logger_uploader,
                S3_BUCKET=bucket,
                S3_PREFIX=s3_prefix,
                failed_folder=FAILED_LOGS,
                is_logs=True
            )