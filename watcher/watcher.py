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
import logging

load_dotenv()

logger = setup_logger("dropzone.processing")

INCOMING_DIR = os.getenv("INCOMING_DIR")
FAILED_DIR_READ = os.getenv("FAILED_DIR_READ")
FAILED_DIR_TRANSFORM = os.getenv("FAILED_DIR_TRANSFORM")

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
    logger.info("Processing: %s", file_path)

    for attempt in range(3):
        try:
            df = pd.read_csv(file_path)
            logger.info("✅ CSV's been successfully read: %s", file_path)
            break
        except Exception as e:
            logger.warning("🌀 Read csv failed. Path: %s, Attempt NO %d", file_path, attempt)
            time.sleep(30)
        
    else:
        logger.error("❗Read csv permaently failed: %s", file_path)
        failed_path_r = os.path.join(FAILED_DIR_READ, os.path.basename(file_path))
        try:
            os.replace(file_path, failed_path_r)
            logger.info("File moved to failed/read: %s", failed_path_r)
        except Exception as e:
            logger.warning("🟡 STUCK IN INCOMING FOLDER! Failed to move to failed/read: %s", file_path)
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

    os.makedirs("processed", exist_ok=True)

    fname = f"transactions_{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex}.parquet"

    for attempt in range(3):
        try:
            df.to_parquet(f"processed/{fname}", index=False)
            logger.info("✅ Parquet is ready in processed folder: %s", fname)
            break
        except Exception as e:
            logger.warning("🌀 Failed to write parquet: %s, Attempt NO %d", fname, attempt)
            time.sleep(30)

    else:
        logger.error("❗Write parquet permaently failed: %s", fname)
        failed_path_t = os.path.join(FAILED_DIR_TRANSFORM, os.path.basename(file_path))
        try:
            os.replace(file_path, failed_path_t)
            logger.info("File moved to failed/transorm: %s", failed_path_t)
        except Exception as e:
            logger.warning("🟡 STUCK IN INCOMING FOLDER! Failed to move to failed/transform: %s", file_path)
        return


class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)

        if file_name.startswith(".") or file_name.endswith(".tmp"):
            logger.info("🌀 Ignoring tmp file: %s", file_path)
            return
        
        if not file_name.endswith(".csv"):
            logger.info("🌀 Ignoring file - not csv: %s", file_path)
            return
        

if __name__ == "__main__":

    os.makedirs(INCOMING_DIR, exist_ok=True)

    observer = Observer()
    observer.schedule(FileHandler(), INCOMING_DIR, recursive=False)
    observer.start()
    print("Watching:", os.path.abspath(INCOMING_DIR))
    logger.info("Watching: %s", os.path.abspath(INCOMING_DIR))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Keyboard interruption. Watcher has been stopped")
    observer.join()
    

