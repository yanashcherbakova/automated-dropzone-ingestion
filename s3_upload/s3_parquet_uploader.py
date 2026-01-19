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

from aws.s3_utils import s3_cfg, build_s3, utcnow, s3_key, upload_to_s3

load_dotenv()
logger = setup_logger("dropzone.uploader")

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")
PROCESSED_DIR = os.getenv("PROCESSED_DIR")
FAILED_DIR_UPLOAD = os.getenv("FAILED_DIR_UPLOAD")

if not S3_BUCKET:
    logger.error("❗S3_Bucket unavaliable")
    raise SystemExit("S3_Bucket is required")


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

        upload_to_s3(file_path, logger, AWS_REGION, S3_BUCKET, FAILED_DIR_UPLOAD, False)
        
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
        
                                     
