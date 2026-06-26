"""
Cosmos DB Emulator Data Loader
Loads JSON data files into the local emulator with retry on timeout.
Runs externally (not via init scripts) so the emulator stays running.

Usage:
    python3 -m pip install azure-cosmos
    python3 load-data.py --endpoint https://localhost:8081 --key <key> \
        --database database-v2 \
        --data-dir ../mslearn-cosmosdb-modules-central-main/data/fullset/database-v2
"""

import argparse
import json
import os
import time
import sys
import warnings
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from azure.cosmos import CosmosClient, PartitionKey, exceptions

RETRY_ATTEMPTS = 5
RETRY_DELAY_S  = 2   # seconds between retries

def upsert_with_retry(container, item):
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            container.upsert_item(item)
            return True
        except exceptions.CosmosHttpResponseError as e:
            if attempt < RETRY_ATTEMPTS:
                print(f"    [retry {attempt}/{RETRY_ATTEMPTS-1}] {e.status_code} – waiting {RETRY_DELAY_S}s...")
                time.sleep(RETRY_DELAY_S)
            else:
                print(f"    [FAILED] id={item.get('id')} – {e.message}")
                return False
        except Exception as e:
            if attempt < RETRY_ATTEMPTS:
                print(f"    [retry {attempt}/{RETRY_ATTEMPTS-1}] {e} – waiting {RETRY_DELAY_S}s...")
                time.sleep(RETRY_DELAY_S)
            else:
                print(f"    [FAILED] id={item.get('id')} – {e}")
                return False

def load_file(container, filepath):
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        data = [data]

    ok = failed = 0
    for i, item in enumerate(data, 1):
        if i % 500 == 0:
            print(f"    {i:,}/{len(data):,} ...", flush=True)
        if upsert_with_retry(container, item):
            ok += 1
        else:
            failed += 1
    return ok, failed

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database",  required=True, help="Database name, e.g. database-v2")
    parser.add_argument("--data-dir",  required=True, help="Path to folder with .json files")
    parser.add_argument("--endpoint", required=True, help="Cosmos DB endpoint, e.g. https://localhost:8081")
    parser.add_argument("--key",      required=True, help="Cosmos DB account key")
    parser.add_argument("--container", default=None, help="Load only this container (optional)")
    args = parser.parse_args()

    client = CosmosClient(
        args.endpoint, args.key,
        connection_verify=False   # emulator uses self-signed cert
    )

    db = client.get_database_client(args.database)

    files = sorted(f for f in os.listdir(args.data_dir) if f.endswith(".json"))
    if args.container:
        files = [f for f in files if f == f"{args.container}.json"]

    total_ok = total_failed = 0
    for filename in files:
        container_name = filename[:-5]   # strip .json
        filepath = os.path.join(args.data_dir, filename)

        container = db.get_container_client(container_name)
        print(f"\n→ {container_name} ({filename})")

        ok, failed = load_file(container, filepath)
        total_ok += ok
        total_failed += failed
        print(f"  ✓ {ok:,}  ✗ {failed}")

    print(f"\nDone. Total: {total_ok:,} ok, {total_failed:,} failed")
    if total_failed:
        sys.exit(1)

if __name__ == "__main__":
    main()
