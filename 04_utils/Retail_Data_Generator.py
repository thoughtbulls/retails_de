# Databricks notebook source
import json
import random
import os
from datetime import datetime, timedelta
import uuid

BASE_PATH = "raw_data"

stores = ["MATH001", "AGRA002", "PUNE010", "DELHI999"]
items = ["ITM101", "ITM102", "ITM200", "ITM500", "ITM900"]
payments = ["UPI", "CARD", "CASH"]

def generate_record(base_time):
    record = {
        "order_id": f"ORD{random.randint(1000, 9999)}",
        "store_id": random.choice(stores),
        "txn_time": (base_time - timedelta(minutes=random.randint(0, 120))).isoformat(),
        "item_id": random.choice(items),
        "qty": random.randint(1, 5),
        "price": round(random.uniform(50, 2000), 2),
        "payment_type": random.choice(payments)
    }

    rand = random.random()

    # Inject issues
    if rand < 0.1:
        record["order_id"] = None  # null key

    elif rand < 0.2:
        record["qty"] = "two"  # wrong type

    elif rand < 0.3:
        record["price"] = -100  # invalid value

    elif rand < 0.4:
        record["new_column"] = "schema_drift"  # new field

    elif rand < 0.45:
        record["txn_time"] = "invalid_date"  # bad timestamp

    return record


def generate_file(path, num_records, base_time):
    os.makedirs(path, exist_ok=True)

    filename = f"{path}/sales_{uuid.uuid4().hex}.json"

    with open(filename, "w") as f:
        records = []

        for _ in range(num_records):
            rec = generate_record(base_time)
            records.append(rec)

            # duplicate records
            if random.random() < 0.05:
                records.append(rec)

        for rec in records:
            if random.random() < 0.02:
                f.write('{"corrupt_json": ')  # broken line
            else:
                f.write(json.dumps(rec) + "\n")

    return filename


def generate_data(days=2, hours=4, files_per_hour=3, records_per_file=5000):
    base_time = datetime.now()

    all_files = []

    for d in range(days):
        date = (base_time - timedelta(days=d)).strftime("%Y-%m-%d")

        for h in range(hours):
            hour_path = f"{BASE_PATH}/date={date}/hour={h:02d}"

            for _ in range(files_per_hour):
                file = generate_file(hour_path, records_per_file, base_time)
                all_files.append(file)

                # simulate duplicate file ingestion
                if random.random() < 0.1:
                    duplicate_path = file.replace(".json", "_dup.json")
                    with open(file, "r") as src, open(duplicate_path, "w") as dst:
                        dst.write(src.read())

    print(f"Generated {len(all_files)} files")


# Run generator
generate_data(days=2, hours=6, files_per_hour=4, records_per_file=5000)