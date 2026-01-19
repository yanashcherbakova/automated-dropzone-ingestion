from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os
from uuid import uuid4
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from synth_data.values import CORRECT_COLUMN_NAMES, CURRENCY_MAPPING, VALID_CURRENCIES, CANONICAL_STATUS, STATUS_MAPPING, CANONICAL_PAYMENT_METHODS, PAYMENT_METHOD_MAPPING
from logging_config import setup_logger
from aws.s3_utils import build_s3, s3_cfg

from log_shipper import ship_rotated_logs_loop
import threading


load_dotenv()

logger_ingest = setup_logger("dropzone.reading")
logger = setup_logger("dropzone.processing")
logger_uploader = setup_logger("dropzone.uploader")

INCOMING_DIR = os.getenv("INCOMING_DIR")
PROCESSED_DIR = os.getenv("PROCESSED_DIR")
FAILED_DIR_READ = os.getenv("FAILED_DIR_READ")
FAILED_DIR_TRANSFORM = os.getenv("FAILED_DIR_TRANSFORM")

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
s3 = build_s3(AWS_REGION, s3_cfg)

t = threading.Thread(target=ship_rotated_logs_loop, args=(s3, S3_BUCKET, logger_uploader), daemon=True)
t.start()

 #"transaction_id",
 #"transaction_ts",
  #"user_id",
 #"amount",
 #"currency",
 #"status",
 #"product_id",
 #"payment_method"

def process_file(file_path):
    print("PROCESS:", file_path)
    logger_ingest.info("Processing: %s", file_path)

    for attempt in range(1,4):
        try:
            df = pd.read_csv(file_path)
            logger_ingest.info("✅ CSV's been successfully read: %s", file_path)
            break
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
    
    if len(CORRECT_COLUMN_NAMES )== df.shape[1]:
        df.columns = CORRECT_COLUMN_NAMES

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


    os.makedirs(PROCESSED_DIR, exist_ok=True)

    fname = f"transactions_{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex}.parquet"
    tmp_path = os.path.join(PROCESSED_DIR, "." + fname + ".tmp")
    p_path = os.path.join(PROCESSED_DIR, fname)

    for attempt in range(1,4):
        try:
            df.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, p_path)
            logger.info("✅ Parquet is ready in processed folder: %s", fname)
            break
        except Exception as e:
            logger.warning("🌀 Failed to write parquet: %s, Attempt NO %d", fname, attempt)
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
            time.sleep(30)

    else:
        logger.error("❗Write parquet permaently failed: %s", fname)
        failed_path_t = os.path.join(FAILED_DIR_TRANSFORM, os.path.basename(file_path))
        try:
            os.replace(file_path, failed_path_t)
            logger.info("File moved to failed/transform: %s", failed_path_t)
        except Exception as e:
            logger.warning("🟡 STUCK IN INCOMING FOLDER! Failed to move to failed/transform: %s", file_path)
        return


class IngestingFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)

        if file_name.startswith(".") or file_name.endswith(".tmp"):
            logger_ingest.info("🌀 Ignoring tmp file: %s", file_path)
            return
        
        if not file_name.endswith(".csv"):
            logger_ingest.info("🌀 Ignoring file - not csv: %s", file_path)
            return
        
        logger_ingest.info("✅ CSV File detected: %s", file_path)
        process_file(file_path)
        

if __name__ == "__main__":

    os.makedirs(INCOMING_DIR, exist_ok=True)

    observer = Observer()
    observer.schedule(IngestingFileHandler(), INCOMING_DIR, recursive=False)
    observer.start()
    print("Watching:", os.path.abspath(INCOMING_DIR))
    logger_ingest.info("Watching: %s", os.path.abspath(INCOMING_DIR))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger_ingest.info("🌀Keyboard interruption. Watcher /incoming has been stopped")
    observer.join()
    

