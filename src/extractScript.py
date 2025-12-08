import os
import math
import asyncio
import aiohttp
from dotenv import load_dotenv
from Catalog import extractions
import pandas as pd
import threading
from datetime import datetime
import boto3
from botocore.client import Config
from io import BytesIO

load_dotenv()

api_local_key = os.getenv("API_LOCAL_KEY")
api_key = os.getenv("APY_KEY")
password_db = os.getenv("PASSWORD_DB")
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

async def extract_data(name):
    prj_id = extractions[name]["project_id"]
    subprj_id = extractions[name]["subproject_id"]
    logic_name = extractions[name]["name"]
    query = extractions[name].get("query", "")

    base_url = f"https://dpms-api.rc1.kaefer.com/api/v1/{logic_name}?project_id={prj_id}&subproject_id={subprj_id}{query}"
    headers = {"Authorization": f"Token {api_key}"}
    all_data = []

    async with aiohttp.ClientSession() as session:
        # Fetch first page to get total and page size
        first_url = f"{base_url}&skip=0"
        first_response = await fetch_page(session, first_url, headers)
        total = first_response.get('total', 1)
        data = first_response.get('data', [])
        all_data.extend(data)

        page_size = len(data)
        if page_size == 0:
            log(f"[{name}] No data returned on first page.")
            return

        log(f"[{name}] Total records: {total}, page_size: {page_size}")

        # Calculate number of pages
        page_count = math.ceil(total / page_size)

        # Prepare skip values for all remaining pages
        skip_values = [i * page_size for i in range(1, page_count)]

        # Fetch all remaining pages in batches
        batches = [
            skip_values[i:i + page_size * MAX_CONCURRENT_BATCHES]
            for i in range(0, len(skip_values), page_size * MAX_CONCURRENT_BATCHES)
        ]

        for batch in batches:
            tasks = [
                fetch_page(session, f"{base_url}&skip={skip}", headers)
                for skip in batch
            ]
            results = await asyncio.gather(*tasks)
            for resp in results:
                all_data.extend(resp.get('data', []))
            log(f"[{name}] Processed batch: skips {batch}")

    # Convert to DataFrame
    df = pd.json_normalize(all_data)
    parquet_buffer = BytesIO()
    OBJECT_PREFIX = 'dpms'
    OBJECT_KEY_PARQUET = f"{OBJECT_PREFIX}/{name}.parquet"
    OBJECT_KEY_JSON = f"{OBJECT_PREFIX}/{name}.json"

    # Try to save as Parquet, else save as JSON
    try:
        df.to_parquet(parquet_buffer, index=False)
        parquet_bytes = parquet_buffer.getvalue()
        file_type = "parquet"
        file_bytes = parquet_bytes
        object_key = OBJECT_KEY_PARQUET
        log(f"[{name}] Parquet conversion successful.")
    except Exception as e:
        log(f"[{name}] ❌ Error converting to Parquet: {e}")
        log(f"[{name}] DataFrame dtypes: {df.dtypes}")
        log(f"[{name}] Sample data: {df.head().to_dict()}")
        # Save as JSON instead
        json_buffer = BytesIO()
        # Use orient="records" for row-wise JSON
        json_str = df.to_json(orient="records", force_ascii=False)
        json_buffer.write(json_str.encode("utf-8"))
        json_buffer.seek(0)
        file_type = "json"
        file_bytes = json_buffer.getvalue()
        object_key = OBJECT_KEY_JSON
        log(f"[{name}] Saved as JSON.")

    # MinIO config
    minioPassword = os.getenv("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = "http://10.247.81.220:29000"
    MINIO_ACCESS_KEY = "admin"
    MINIO_SECRET_KEY = minioPassword
    MINIO_REGION = "kaefer-bra-indaiatuba"
    LANDING_BUCKET = "landing"

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            endpoint_url=MINIO_ENDPOINT,
            region_name=MINIO_REGION,
            config=Config(signature_version='s3v4')
        )

        # Upload the file (either Parquet or JSON)
        s3_client.put_object(
            Bucket=LANDING_BUCKET,
            Key=object_key,
            Body=file_bytes,
            ContentType='application/octet-stream' if file_type == "parquet" else 'application/json'
        )
        log(f"[{name}] ✅ Uploaded {file_type.upper()} to MinIO: s3://{LANDING_BUCKET}/{object_key}")
        log(f"[{name}] Total records uploaded: {len(all_data)}")

    except Exception as e:
        log(f"[{name}] ❌ Error uploading {file_type.upper()} to MinIO: {e}")

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
