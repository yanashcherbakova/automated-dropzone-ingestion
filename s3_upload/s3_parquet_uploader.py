from logging_config import setup_logger
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError
import time
import boto3

import datetime

load_dotenv()
logger = setup_logger("dropzone.uploader")

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")
PROCESSED_DIR = os.getenv("PROCESSED_DIR")
FAILED_DIR_UPLOAD = os.getenv("FAILED_DIR_UPLOAD")

s3_cfg = Config(retries={"max_attemts": 10, "mode" : "standart"})

if not S3_BUCKET:
    logger.error("❗S3_Bucket unavaliable")
    raise SystemExit("S3_Bucket is required")

def build_s3():
    session = boto3.Session(region_name = AWS_REGION)
    return session.client("s3", config = s3_cfg)

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)

def s3_key(now, file_name):
    return f"{S3_PREFIX}/year={now:%Y}/month={now:%m}/day={now:%d}/{file_name}"

def upload_to_s3(file_path):
    logger.info("Processing: %s", file_path)

    s3 = build_s3()
    key = s3_key(utcnow())

    try:
        s3.upload_file(file_path, Bucket= S3_BUCKET, Key = key)
        logger.info("✅ Uploaded to S3 %s", key)
    except (ClientError, BotoCoreError) as e:
        logger.warning("❗Upload to S3 failed %s", key, exc_info=True)
        failed_path_upload = os.path.join(FAILED_DIR_UPLOAD, os.path.basename(file_path))
        try:
            os.replace(file_path, failed_path_upload)
            logger.info("File moved to failed/upload: %s", failed_path_upload)
        except Exception as e:
            logger.warning("🟡 STUCK IN PROCESSED FOLDER! Failed to move to failed/upload: %s", file_path)
            return
    else:
        try:
            os.remove(file_path)
            logger.info("✅ Uploaded and removed: %s", file_path)
        except FileNotFoundError:
            logger.warning("🟡 Uploaded but file already missing: %s", file_path)
        except Exception:
            logger.warning("🟡 Uploaded but failed to remove: %s", file_path, exc_info=True)


class ProcessedFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)

        if file_name.startswith(".") or file_name.endswith(".tmp"):
            logger.info("🌀 Ignoring tmp file: %s", file_path)
            return
        
        if not file_name.endswith(".parquet"):
            logger.info("🌀 Ignoring file - not parquet: %s", file_path)
            return
        
        logger.info("✅ Parquet File detected: %s", file_path)

        upload_to_s3(file_path)
        
if __name__ == "__maint__":
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    observer = Observer()
    observer.schedule(ProcessedFileHandler(), PROCESSED_DIR, recursive=False)
    observer.start()
    print("Watching:", os.path.abspath(PROCESSED_DIR))
    logger.info("Watching: %s", os.path.abspath(PROCESSED_DIR))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("🌀 Keyboard interruption. Watcher /processed has been stopped")
    observer.join()
        
                                     
