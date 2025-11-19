import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from Catalog import extractions
import pandas as pd
import json
import threading
from datetime import datetime
import boto3
from botocore.client import Config
from io import StringIO

load_dotenv()

api_key = os.getenv("APY_KEY")
password_db = os.getenv("PASSWORD_DB")
BATCH_SIZE = 150
MAX_CONCURRENT_BATCHES = 4
TIMEOUT = 30

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

async def fetch_page(session, url, headers):
    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            resp.raise_for_status()
            return await resp.json()
    except asyncio.TimeoutError:
        log(f"Timeout while connecting to {url}")
        return {"data": []}
    except Exception as e:
        log(f"Error fetching {url}: {e}")
        return {"data": []}

async def process_batch(session, batch_urls, headers, name, batch_start, batch_end, last_page):
    log(f"[{name}] Starting batch {batch_start}-{batch_end}")
    tasks = [fetch_page(session, url, headers) for url in batch_urls]
    results = await asyncio.gather(*tasks)
    batch_data = []
    for j, resp in enumerate(results):
        batch_data.extend(resp.get('data', []))
    log(f"[{name}] Finished batch {batch_start}-{batch_end}")
    return batch_data

async def extract_data(name):
    prj_id = extractions[name]["project_id"]
    subprj_id = extractions[name]["subproject_id"]
    logic_name = extractions[name]["name"]
    query = extractions[name].get("query", "")

    base_url = f"https://dpms-api.rc1.kaefer.com/api/v1/{logic_name}?project_id={prj_id}&subproject_id={subprj_id}{query}&page="
    headers = {"Authorization": f"Token {api_key}"}

    full_extraction = []

    async with aiohttp.ClientSession() as session:
        first_url = base_url + "1"
        first_response = await fetch_page(session, first_url, headers)
        last_page = first_response.get('total', 1)
        log(f"[{name}] Request succeeded - processing {last_page} pages...")

        page_urls = [base_url + str(page) for page in range(1, last_page + 1)]
        batches = [
            (page_urls[i:i + BATCH_SIZE], i + 1, min(i + BATCH_SIZE, last_page))
            for i in range(0, len(page_urls), BATCH_SIZE)
        ]

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

        async def batch_wrapper(batch_urls, batch_start, batch_end):
            async with semaphore:
                return await process_batch(session, batch_urls, headers, name, batch_start, batch_end, last_page)

        batch_tasks = [batch_wrapper(batch_urls, batch_start, batch_end) for batch_urls, batch_start, batch_end in batches]
        for future in asyncio.as_completed(batch_tasks):
            batch_data = await future
            full_extraction.extend(batch_data)

    # Convert to DataFrame and CSV (in memory)
    df = pd.json_normalize(full_extraction)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    # MinIO config
    minioPassword = os.getenv("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = "http://10.247.81.220:29000"
    MINIO_ACCESS_KEY = "admin"
    MINIO_SECRET_KEY = minioPassword
    MINIO_REGION = "kaefer-bra-indaiatuba"
    LANDING_BUCKET = "landing"
    OBJECT_PREFIX = 'dpms'
    OBJECT_KEY = f"{OBJECT_PREFIX}/{name}.csv"

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT,
            region_name=MINIO_REGION,
            config=Config(signature_version='s3v4')
        )

        # Upload the CSV bytes
        s3_client.put_object(
            Bucket=LANDING_BUCKET,
            Key=OBJECT_KEY,
            Body=csv_bytes,
            ContentType='text/csv'
        )
        log(f"[{name}] ✅ Uploaded CSV to MinIO: s3://{LANDING_BUCKET}/{OBJECT_KEY}")
        log(f"[{name}] Total records uploaded: {len(full_extraction)}")

    except Exception as e:
        log(f"[{name}] ❌ Error uploading CSV to MinIO: {e}")

async def run_extraction_thread(name):
    await extract_data(name)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        name = sys.argv[1]
        asyncio.run(run_extraction_thread(name))
    else:
        threads = []
        for name in extractions.keys():
            t = threading.Thread(target=lambda: asyncio.run(run_extraction_thread(name)))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        log("All extractions completed.")
