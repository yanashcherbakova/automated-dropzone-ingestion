from logging_config import setup_logger
from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler
import os
import time
import threading
from aws.s3_utils import s3_cfg, build_s3
from s3_upload import uploader_worker as upw

load_dotenv()

USE_POLLING = os.getenv("WATCHDOG_POLLING", "0") == "1"
if USE_POLLING:
    from watchdog.observers.polling import PollingObserver as Observer
else:
    from watchdog.observers import Observer

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

upw.init_context(
    logger_uploader,
    logger_ingest,
    AWS_REGION,
    S3_BUCKET,
    S3_PREFIX,
    PROCESSED_DIR,
    FAILED_DIR_UPLOAD,
    s3
)

class ProcessedFileHandler(FileSystemEventHandler):
    def _handle(self, path):
        if path.endswith(".tmp"):
            return
        if not path.endswith(".parquet"):
            return
        upw.queue_file(path, source="watchdog // processed_folder")

    def on_created(self, event):
        if not event.is_directory:
            self._handle(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._handle(event.dest_path)
        
if __name__ == "__main__":
    import socket, os, sys, signal

    logger_uploader.info(
        "BOOT env | host=%s | cwd=%s | python=%s",
        socket.gethostname(),
        os.getcwd(),
        sys.executable
    )

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR_UPLOAD, exist_ok=True)

    stop_event = threading.Event()

    thr_upload = threading.Thread(target=upw.uploader_worker, args=(stop_event,))
    thr_upload.start()

    thr_rescan = threading.Thread(target=upw.processed_rescan_loop, args=(stop_event, FAILED_DIR_UPLOAD, 60))
    thr_rescan.start()

    thr_rescan_processed = threading.Thread(target=upw.processed_rescan_loop, args=(stop_event, PROCESSED_DIR, 60))
    thr_rescan_processed.start()


    logger_ingest.info("🌀 Rescan of FAILED/UPLOAD folder (every 60 sec) started")

    observer = Observer()
    observer.schedule(ProcessedFileHandler(), PROCESSED_DIR, recursive=False)
    observer.start()
    logger_ingest.info("🌀 Watching: %s", os.path.abspath(PROCESSED_DIR))

    def request_shutdown(signum, frame):
        logger_ingest.info("🌀 Shutdown signal %s: stopping observer + uploader threads", signum)
        stop_event.set()
        observer.stop()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        request_shutdown(signal.SIGINT, None)

    observer.join()
    thr_rescan.join()
    thr_rescan_processed.join()
    thr_upload.join()

                                     
