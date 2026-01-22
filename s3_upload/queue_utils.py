import os
import threading
from queue import Full, Queue, Empty
from aws.s3_utils import s3_cfg, build_s3, upload_to_s3
import time
from dotenv import load_dotenv
import glob

load_dotenv()

PROCESSED_DIR = os.getenv("PROCESSED_DIR")
upload_queue = Queue(maxsize=2000)
claimed_files = set()
claimed_files_lock = threading.Lock()

def is_candidate(file_path):
    name = os.path.basename(file_path)
    if name.startswith(".") or name.endswith(".tmp"):
        return False
    if not name.endswith(".parquet"):
        False

def claim_file(file_path):
    if not is_candidate(file_path):
        return False
    
    claimed_files_lock.aquire()
    try:
        if file_path in claimed_files:
            return False
        claimed_files.add(file_path)
        return True
    finally:
        claimed_files_lock.release()

def release_claim(file_path):
    claimed_files_lock.acquire()
    try:
        claimed_files.discard(file_path)
    finally:
        claimed_files_lock.release()

def queue_file(file_path, logger_ingest, source):
    if not is_candidate(file_path):
        return False
    
    try:
        upload_queue.put(file_path, timeout=1)
        logger_ingest.info("-- Queued (%s): %s", source, file_path)
        return True
    except Full:
        logger_ingest.warning("🟡 Queue full, could not enqueue: %s", file_path, exc_info=True)
        release_claim(file_path)
        return False
    
    except Exception:
        logger_ingest.warning("🟡 Adding to queue failed: %s", file_path, exc_info=True)
        release_claim(file_path)
        return False
    
def uploader_worker(stop_event, logger_uploader):
    logger_uploader.info("--Uploader worker started")

    while not stop_event.is_set():
        try:
            file_path = upload_queue.get(timeout=1)
        except Empty:
            continue

        try:
            logger_uploader.info("--Start upload: %s", file_path)
            upload_to_s3(
                s3, 
                file_path, 
                logger_uploader, 
                S3_BUCKET, 
                S3_PREFIX, 
                FAILED_DIR_UPLOADS, 
                is_logs=False)
            logger_uploader.info("✅ Finished upload handling: %s", file_path)
        finally:
            upload_queue.task_done()
            release_claim(file_path)
            return False
            

def processed_rescan_loop(stop_event, logger_uploader):
    while not stop_event.is_set():
        pattern = os.path.join(PROCESSED_DIR, "*.parquet")
        found = 0
        queued = 0

        for file_path in sorted(glob.glob(pattern)):
            found += 1
            if queue_file(file_path, logger_ingest, source="rescan"):
                queued += 1

        stop_event.wait(60)