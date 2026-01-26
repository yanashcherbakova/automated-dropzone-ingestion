import os
from queue import Full


def is_candidate(file_path, target):
    name = os.path.basename(file_path)
    if name.startswith(".") or name.endswith(".tmp"):
        return False
    if not name.endswith(target):
        return False
    return True

def claim_file(file_path, set_LOCKER, claimed_set, target):
    if not is_candidate(file_path, target):
        return False
    
    set_LOCKER.acquire()
    try:
        if file_path in claimed_set:
            return False
        claimed_set.add(file_path)
        return True
    finally:
        set_LOCKER.release()

def release_claim(file_path, set_LOCKER, claimed_set):
    set_LOCKER.acquire()
    try:
        claimed_set.discard(file_path)
    finally:
        set_LOCKER.release()

def queue_file(file_path, general_queue, logger, source, set_LOCKER, claimed_set, target):
    if not claim_file(file_path, set_LOCKER, claimed_set, target):
        return False
    
    try:
        general_queue.put(file_path, timeout=1)
        logger.info("-- Queued (%s): %s", source, file_path)
        return True
    except Full:
        logger.warning("🟡 Queue full, could not enqueue: %s", file_path, exc_info=True)
        release_claim(file_path, set_LOCKER, claimed_set)
        return False
    
    except Exception:
        logger.warning("🟡 Adding to queue failed: %s", file_path, exc_info=True)
        release_claim(file_path, set_LOCKER, claimed_set)
        return False
