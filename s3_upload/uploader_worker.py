import os
import threading
from queue import Queue, Empty
from aws.s3_utils import upload_to_s3
import glob
import utils.queue_utils as utils

upload_queue = Queue(maxsize=2000)
claimed_files = set()
claimed_files_lock = threading.Lock()

TARGET_EXT = ".parquet" 

logger_uploader = None
logger_ingest = None

AWS_REGION = None
S3_BUCKET = None
S3_PREFIX = None
PROCESSED_DIR = None
FAILED_DIR_UPLOAD = None
s3 = None

def init_context(
        logger_uploader_main,
        logger_ingest_main,
        aws_region,
        s3_bucket,
        s3_prefix,
        processed_dir,
        failed_dir_upload,
        s3_built,
):
    global logger_uploader, logger_ingest
    global AWS_REGION, S3_BUCKET, S3_PREFIX, PROCESSED_DIR, FAILED_DIR_UPLOAD, s3

    logger_uploader = logger_uploader_main
    logger_ingest = logger_ingest_main

    AWS_REGION=aws_region
    S3_BUCKET=s3_bucket
    S3_PREFIX=s3_prefix
    PROCESSED_DIR=processed_dir
    FAILED_DIR_UPLOAD=failed_dir_upload
    s3 = s3_built

def queue_file(file_path, source):
    return utils.queue_file(
        file_path=file_path,
        general_queue=upload_queue,
        logger=logger_ingest,
        source=source,
        set_LOCKER=claimed_files_lock,
        claimed_set=claimed_files,
        target=TARGET_EXT,
    )

def uploader_worker(stop_event):
    logger_uploader.info("--Uploader worker started")
    while True:
        if stop_event.is_set() and upload_queue.empty():
            break
        try:
            file_path = upload_queue.get(timeout=1)
        except Empty:
            continue

        try:
            logger_uploader.info("🌀 --Start upload: %s", file_path)
            upload_to_s3(
                s3, 
                file_path, 
                logger_uploader, 
                S3_BUCKET, 
                S3_PREFIX, 
                FAILED_DIR_UPLOAD, 
                is_logs=False)
            logger_uploader.info("✅ Finished upload handling: %s", file_path)
        finally:
            upload_queue.task_done()
            utils.release_claim(file_path, claimed_files_lock, claimed_files)
            

def processed_rescan_loop(stop_event):
    logger_uploader.info("🌀-- Rescan of FAILED/UPLOAD FOLDER --🌀")
    while not stop_event.is_set():
        pattern = os.path.join(FAILED_DIR_UPLOAD, "*.parquet")
        found = 0
        queued = 0

        for file_path in sorted(glob.glob(pattern)):
            if stop_event.is_set():
                break
            found += 1
            logger_uploader.info("🟡 Located parquet-file in FAILED/UPLOAD FOLDER: %s", file_path)
            if queue_file(file_path, source="rescan"):
                logger_uploader.info("🟣 FAILED file was queued for upload: %s", file_path)
                queued += 1

        stop_event.wait(60)