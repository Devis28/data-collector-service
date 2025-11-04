import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Načítaj .env súbor (musí byť v root adresári)
load_dotenv(dotenv_path=".env")

R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_KEY_ID = os.getenv("R2_KEY_ID")
R2_SECRET = os.getenv("R2_SECRET")
R2_BUCKET = os.getenv("R2_BUCKET")

# Inicializácia klienta pre Cloudflare R2
session = boto3.session.Session()
client = session.client(
    's3',
    region_name='auto',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_KEY_ID,
    aws_secret_access_key=R2_SECRET
)

def upload_json_to_r2(local_file_path, r2_key):
    """
    Uploaduje lokálny JSON súbor na Cloudflare R2.

    :param local_file_path: cesta k lokálnemu súboru
    :param r2_key: cieľový klúč v R2 (napr. bronze/ROCK/song/04-11-2025/16-16-20.json)
    :return: True ak upload prebehol úspešne, inak False
    """
    try:
        with open(local_file_path, 'rb') as f:
            client.upload_fileobj(f, R2_BUCKET, r2_key)
        print(f"Upload OK: {local_file_path} -> {R2_BUCKET}/{r2_key}")
        return True
    except ClientError as e:
        print(f"Upload ERROR: {local_file_path} -> {R2_BUCKET}/{r2_key} :: {e}")
        return False
    except Exception as e:
        print(f"General ERROR during upload {local_file_path}: {e}")
        return False
