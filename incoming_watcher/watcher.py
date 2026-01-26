from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os
from dotenv import load_dotenv
from logging_config import setup_logger
from aws.s3_utils import build_s3, s3_cfg

from log_shipper import ship_rotated_logs_loop
import threading
import process_worker as pw

load_dotenv()

logger_ingest = setup_logger("dropzone.reading")
logger_process = setup_logger("dropzone.processing")
logger_uploader = setup_logger("dropzone.uploader")

INCOMING_DIR = os.getenv("INCOMING_DIR")
PROCESSED_DIR = os.getenv("PROCESSED_DIR")
FAILED_DIR_READ = os.getenv("FAILED_DIR_READ")
FAILED_DIR_TRANSFORM = os.getenv("FAILED_DIR_TRANSFORM")

pw.init_context(
    logger_ingest,
    logger_process,
    INCOMING_DIR,
    PROCESSED_DIR,
    FAILED_DIR_READ,
    FAILED_DIR_TRANSFORM,
)

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
s3 = build_s3(AWS_REGION, s3_cfg)

class IngestingFileHandler(FileSystemEventHandler):
    def on_moved(self, event):
        if event.is_directory:
            return
        
        file_path = event.dest_path
        pw.queue_csv(file_path, source= "watchdog // incomong_folder")
        

if __name__ == "__main__":

    os.makedirs(INCOMING_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR_READ, exist_ok=True)
    os.makedirs(FAILED_DIR_TRANSFORM, exist_ok=True)

    stop_processing = threading.Event()

    t_logrotation = threading.Thread(target=ship_rotated_logs_loop, args=(s3, S3_BUCKET, logger_uploader), daemon=True)
    t_logrotation.start()

    t_processing = threading.Thread(target=pw.process_worker, args=(stop_processing,))
    t_processing.start()
    logger_process.info("---Process worker UP---")

    observer = Observer()
    observer.schedule(IngestingFileHandler(), INCOMING_DIR, recursive=False)
    observer.start()
    print("Watching:", os.path.abspath(INCOMING_DIR))
    logger_ingest.info("Watching: %s", os.path.abspath(INCOMING_DIR))


    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger_ingest.info("🌀 Keyboard interrupt: stopping observer + process worker")
        observer.stop()
        stop_processing.set()

    observer.join()
    t_processing.join()

    

