from botocore.config import Config
import boto3
from botocore.exceptions import ClientError, BotoCoreError
import datetime
import os

s3_cfg = Config(retries={"max_attempts": 10, "mode" : "standard"})

def build_s3(AWS_REGION, s3_cfg):
    session = boto3.Session(region_name = AWS_REGION)
    return session.client("s3", config = s3_cfg)

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)

def s3_key(S3_PREFIX, now, file_name, is_logs=False):
    prefix = "logs" if is_logs else S3_PREFIX
    date_path = f"year={now:%Y}/month={now:%m}/day={now:%d}"

    if prefix == "logs":
        date_path = f"{date_path}/hour={now:%H}"

    return f"{prefix}/{date_path}/{file_name}"


def upload_to_s3(s3, file_path, logger_uploader, S3_BUCKET, S3_PREFIX, failed_folder, is_logs = False):
    logger_uploader.info("Processing: %s", file_path)
    file_name = os.path.basename(file_path)
    
    key = s3_key(S3_PREFIX, utcnow(), file_name, is_logs)

    try:
        s3.upload_file(file_path, S3_BUCKET, key)
        logger_uploader.info("✅ Uploaded to S3 %s", key)
    except (ClientError, BotoCoreError) as e:
        logger_uploader.warning("🔴 Upload to S3 failed %s", key, exc_info=True)
        failed_path_upload = os.path.join(failed_folder, file_name)
        try:
            os.replace(file_path, failed_path_upload)
            logger_uploader.info("🔴 Parquet was moved to FAILED/UPLOAD %s", key, exc_info=True)
        except Exception as e:
            if is_logs:
                logger_uploader.warning("🟡 STUCK IN LOGS FOLDER! Failed to move to failed/log_upload: %s", file_path)
                return

            logger_uploader.warning("🟡 STUCK IN PROCESSED FOLDER! Failed to move to failed: %s", file_path)
            return
    else:
        try:
            os.remove(file_path)
            logger_uploader.info("✅ Uploaded and removed: %s", file_path)
        except FileNotFoundError:
            logger_uploader.warning("🟡 Uploaded but file already missing: %s", file_path)
        except Exception:
            logger_uploader.warning("🟡 Uploaded but failed to remove: %s", file_path, exc_info=True)
