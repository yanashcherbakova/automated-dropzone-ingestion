from logging_config import setup_logger
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import time
import boto3
from s3_upload.queue_utils import queue_file
from queue import Queue
import threading
from aws.s3_utils import s3_cfg, build_s3
from s3_upload.queue_utils import queue_file, uploader_worker, processed_rescan_loop

load_dotenv()
logger_uploader = setup_logger("dropzone.uploader")
logger_ingest = setup_logger("dropzone.reading")

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")
PROCESSED_DIR = os.getenv("PROCESSED_DIR")
FAILED_DIR_UPLOAD = os.getenv("FAILED_DIR_UPLOAD")

if not S3_BUCKET:
    logger_uploader.error("❗S3_Bucket unavaliable")
    raise SystemExit("S3_Bucket is required")

s3 = build_s3(AWS_REGION, s3_cfg)

upload_queue = Queue(maxsize=2000)
claimed_files = set()
claimed_files_lock = threading.Lock()

class ProcessedFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        queue_file(file_path, logger_ingest, source="watchdog",)
        
if __name__ == "__main__":
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR_UPLOAD, exist_ok=True)

    stop_event = threading.Event()

    thr_upload = threading.Thread(target=uploader_worker, args=(stop_event, logger_uploader))
    thr_upload.start()

    observer = Observer()
    observer.schedule(ProcessedFileHandler(), PROCESSED_DIR, recursive=False)
    observer.start()
    print("Watching:", os.path.abspath(PROCESSED_DIR))
    logger_ingest.info("Watching: %s", os.path.abspath(PROCESSED_DIR))

    thr_rescan = threading.Thread(target=processed_rescan_loop, args=(stop_event, logger_uploader))
    thr_rescan.start()
    logger_ingest.info("-- Rescan of PROCESSED FOLDER (every 60 sec) ...")


    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger_ingest.info("🌀 Keyboard interruption. Watcher /processed has been stopped")
        observer.stop()
        stop_event.set()

    observer.join()
    thr_rescan.join()
    thr_upload.join()
        
                                     
