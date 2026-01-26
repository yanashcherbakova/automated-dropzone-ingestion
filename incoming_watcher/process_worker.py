import time
from synth_data.values import CORRECT_COLUMN_NAMES, CURRENCY_MAPPING, VALID_CURRENCIES, CANONICAL_STATUS, STATUS_MAPPING, CANONICAL_PAYMENT_METHODS, PAYMENT_METHOD_MAPPING
import os
import pandas as pd
from queue import Full, Queue, Empty
from datetime import datetime
from uuid import uuid4

from utils.queue_utils import is_candidate

import threading

logger_ingest = None
logger_process = None

INCOMING_DIR = None
PROCESSED_DIR = None
FAILED_DIR_READ = None
FAILED_DIR_TRANSFORM = None

def init_context(
    logger_ingest_main,
    logger_process_main,
    incoming_dir,
    processed_dir,
    failed_dir_read,
    failed_dir_transform,
):
    global logger_ingest, logger_process
    global INCOMING_DIR, PROCESSED_DIR, FAILED_DIR_READ, FAILED_DIR_TRANSFORM

    logger_ingest = logger_ingest_main
    logger_process = logger_process_main

    INCOMING_DIR = incoming_dir
    PROCESSED_DIR = processed_dir
    FAILED_DIR_READ = failed_dir_read
    FAILED_DIR_TRANSFORM = failed_dir_transform

process_queue = Queue(maxsize=2000)
claimed_csv = set()
claimed_csv_lock = threading.Lock()

def claim_csv(file_path):
    if not is_candidate(file_path, target = ".csv"):
        return False
    
    claimed_csv_lock.acquire()
    try:
        if file_path in claimed_csv:
            return False
        claimed_csv.add(file_path)
        return True
    finally:
        claimed_csv_lock.release()
    
def release_claim(file_path):
    claimed_csv_lock.acquire()
    try:
        claimed_csv.discard(file_path)
    finally:
        claimed_csv_lock.release()

def queue_csv(file_path, source):
    if not claim_csv(file_path):
        return False
    
    try:
        process_queue.put(file_path, timeout=1)
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

def read_csv(file_path):
    print("READING:", file_path)
    logger_ingest.info("READING %s", file_path)

    for attempt in range(1,4):
        try:
            df = pd.read_csv(file_path)
            logger_ingest.info("✅ CSV's been successfully read: %s", file_path)
            return df
        except Exception as e:
            logger_ingest.warning("🌀 Read csv failed. Path: %s, Attempt NO %d", file_path, attempt)
            time.sleep(30)
        
    else:
        logger_ingest.error("❗Read csv permaently failed: %s", file_path)
        failed_path_r = os.path.join(FAILED_DIR_READ, os.path.basename(file_path))
        try:
            os.replace(file_path, failed_path_r)
            logger_ingest.info("File moved to failed/read: %s", failed_path_r)
        except Exception as e:
            logger_ingest.warning("🟡 STUCK IN INCOMING FOLDER! Failed to move to failed/read: %s", file_path)
        return
    
def write_tmp_parquet(df,file_path):
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    fname = f"transactions_{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex}.parquet"
    tmp_path = os.path.join(PROCESSED_DIR, "." + fname + ".tmp")
    p_path = os.path.join(PROCESSED_DIR, fname)

    for attempt in range(1,4):
        try:
            df.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, p_path)
            logger_process.info("✅ Parquet is ready in processed folder: %s", fname)

            try:
                os.remove(file_path)
                logger_process.info("✅ Parquet saved / CSV removed: %s", p_path)
                return True
            except FileNotFoundError:
                logger_process.warning("🟡 Parquet saved but CSV file already missing: %s", file_path)
                return True
            except Exception:
                logger_process.warning("🟡 Parquet saved but CSV  failed to remove: %s", file_path, exc_info=True)
                return True
        except Exception as e:
            logger_process.warning("🌀 Failed to write parquet: %s, Attempt NO %d", fname, attempt)
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
            time.sleep(30)
    else:
        logger_process.error("❗Write parquet permaently failed: %s", fname)
        failed_path_t = os.path.join(FAILED_DIR_TRANSFORM, os.path.basename(file_path))
        try:
            os.replace(file_path, failed_path_t)
            logger_process.info("File moved to failed/transform: %s", failed_path_t)
            return False
        except Exception as e:
            logger_process.warning("🟡 STUCK IN INCOMING FOLDER! Failed to move to failed/transform: %s", file_path)
        return False

 #"transaction_id",
 #"transaction_ts",
  #"user_id",
 #"amount",
 #"currency",
 #"status",
 #"product_id",
 #"payment_method"
def process_file(file_path):
    df = read_csv(file_path)
    if df is None:
        return False

    print("PROCESS:", file_path)
    if len(CORRECT_COLUMN_NAMES )== df.shape[1]:
        df.columns = CORRECT_COLUMN_NAMES
    else:
        logger_process.warning("🟡 Unexpected column count (%d) for %s", df.shape[1], file_path)
    
    ts = pd.to_datetime(df.get("transaction_ts"), errors="coerce")
    df = df[ts.notna()]
    df["transaction_ts"] = ts

    mask_today = df["transaction_ts"].dt.date == pd.Timestamp.today().date()
    df = df[mask_today]

    df = df[df["user_id"].notna() & (df["user_id"] != "")]
    df = df[df["currency"].isin(VALID_CURRENCIES) | df["currency"].isin(CURRENCY_MAPPING)]
    df["currency"] = df["currency"].replace(CURRENCY_MAPPING)

    amount = df["amount"]
    mask_str = amount.apply(lambda x: isinstance(x, str))
    amount_cleaned = amount.copy()

    amount_cleaned.loc[mask_str] = (
            amount_cleaned.loc[mask_str]
                .str.strip()
                .str.replace(",", ".", regex=False)
                .astype(float)
            )
    df["amount"] = pd.to_numeric(amount_cleaned, errors="coerce")
    df = df[df["amount"].notna()]
    df = df[df["amount"] >= 0]

    df = df[df["transaction_id"].notna()]
    df = df.drop_duplicates(subset=["transaction_id"])

    df = df[df["status"].isin(CANONICAL_STATUS) | df["status"].isin(STATUS_MAPPING)]
    df["status"] = df["status"].replace(STATUS_MAPPING)

    df = df[df["payment_method"].isin(CANONICAL_PAYMENT_METHODS) | df["payment_method"].isin(PAYMENT_METHOD_MAPPING)]
    df["payment_method"] = df["payment_method"].replace(PAYMENT_METHOD_MAPPING)

    if df.empty:
        logger_process.warning("🟡 All rows filtered out, skipping parquet write: %s", file_path)
        return False

    write_tmp_parquet(df, file_path)


def process_worker(stop_processing):

    logger_process.info("--Process worker started")

    while not stop_processing.is_set():
        try:
            file_path = process_queue.get(timeout=1)
        except Empty:
            continue

        try:
            logger_process.info("--Start process: %s", file_path)
            process_file(file_path)
            logger_process.info("✅ PROCESS WORKER -- finished: %s", file_path)
        finally:
            process_queue.task_done()
            release_claim(file_path)