import os
import threading
from queue import Full, Queue
from aws.s3_utils import s3_cfg, build_s3, upload_to_s3

def is_candidate(file_path):
    name = os.path.basename(file_path)
    if name.startswith(".") or name.endswith(".tmp"):
        return False
    if not name.endswith(".parquet"):
        False

def claim_file(file_path, claimed_files_lock, claimed_files):
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

def queue_files(file_path, source, claimed_files, claimed_files_lock, upload_queue, logger_ingest):
    if not is_candidate(file_path):
        return False
    
    try:
        upload_queue.put(file_path, timeout=1)
        logger_ingest.info("-- Queued (%s): %s", source, file_path)
        return True
    except Full:
        logger_ingest.warning("🟡 Queue full, could not enqueue: %s", file_path, exc_info=True)

        claimed_files_lock.acquire()
        try:
            claimed_files.discard(file_path)
        finally:
            claimed_files_lock.release()
        return False
    except Exception:
        logger_ingest.warning("🟡 Adding to queue failed: %s", file_path, exc_info=True)
        claimed_files_lock.acquire()
        try:
            claimed_files.discard(file_path)
        finally:
            claimed_files_lock.release()
        return False
    
def uploader_worker(stop_event, upload_queue, claimed_files_lock, claimed_files):
    while not stop_event.is_set():
        file_path = upload_queue.get(timeout=1)

        try:
            upload_to_s3(
                s3, 
                file_path, 
                logger_uploader, 
                S3_BUCKET, 
                S3_PREFIX, 
                FAILED_DIR_UPLOADS, 
                is_logs=False)
        finally:
            upload_queue.task_done()
            claimed_files_lock.acquire()
            try:
                claimed_files.discard(file_path)
            finally:
                claimed_files_lock.release()
            return False
            
    



    
