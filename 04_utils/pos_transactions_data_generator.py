import json
import random
import os
from datetime import datetime, timedelta
import uuid

BASE_PATH = "/Volumes/data/raw/sales/raw_data/retail/dev"

stores = ["MATH001", "AGRA002", "PUNE010", "DELHI999"]
items = ["ITM101", "ITM102", "ITM200", "ITM500", "ITM900"]
payments = ["UPI", "CARD", "CASH"]

# generate POS data record, good records along with bad records too
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

# generate POS data file
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


# Generate products data
def generate_products(path, num_records=1000):
    os.makedirs(path, exist_ok=True)
    file = f"{path}/products.json"

    with open(file, "w") as f:
        for i in range(num_records):
            record = {
                "product_id": f"ITM{i}",
                "product_name": f"Product_{i}",
                "category": random.choice(["Fashion", "Grocery", "Electronics"]),
                "price": round(random.uniform(50, 5000), 2)
            }

            # corruption
            if random.random() < 0.1:
                record["price"] = "invalid_price"

            if random.random() < 0.05:
                record["category"] = None

            f.write(json.dumps(record) + "\n")

# Generate stores data
def generate_stores(path):
    os.makedirs(path, exist_ok=True)
    file = f"{path}/stores.json"

    stores = [
        {"store_id": "MATH001", "city": "Mathura", "state": "UP"},
        {"store_id": "AGRA002", "city": "Agra", "state": "UP"},
        {"store_id": "PUNE010", "city": "Pune", "state": "MH"},
        {"store_id": "DELHI999", "city": "Delhi", "state": "DL"}
    ]

    with open(file, "w") as f:
        for rec in stores:
            if random.random() < 0.1:
                rec["city"] = None  # corrupt

            f.write(json.dumps(rec) + "\n")

# Generate customers data
import os, json, random

def generate_customers(path, num_records=2000):
    os.makedirs(path, exist_ok=True)
    file = f"{path}/customers.json"

    records = []

    for i in range(num_records):
        record = {
            "customer_id": f"CUST{i}",
            "name": f"Customer_{i}",
            "phone": str(random.randint(6000000000, 9999999999)),
            "loyalty_points": random.randint(0, 5000)
        }

        # corrupt data scenarios
        if random.random() < 0.1:
            record["phone"] = None

        if random.random() < 0.05:
            record["loyalty_points"] = "high"

        records.append(record)

    # write once (no append)
    with open(file, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

# Generate Inventory data
def generate_inventory(path, num_records=5000):
    os.makedirs(path, exist_ok=True)
    file = f"{path}/inventory.json"

    records = []

    for _ in range(num_records):
        record = {
            "store_id": random.choice(["MATH001", "AGRA002", "PUNE010", "DELHI999"]),
            "product_id": f"ITM{random.randint(0, 1000)}",
            "stock_qty": random.randint(0, 500),
            "last_updated": datetime.now().isoformat()
        }

        # corrupt scenarios
        if random.random() < 0.1:
            record["stock_qty"] = -10

        if random.random() < 0.05:
            record["product_id"] = None

        records.append(record)

    # write once
    with open(file, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")



# Generate POS data
def generate_data(days=2, hours=4, files_per_hour=3, records_per_file=5000):
    base_time = datetime.now()

    all_files = []

    for d in range(days):
        date = (base_time - timedelta(days=d)).strftime("%Y-%m-%d")

        for h in range(hours):
            hour_path = f"{BASE_PATH}/pos_transactions/date={date}/hour={h:02d}"

            for _ in range(files_per_hour):
                file = generate_file(hour_path, records_per_file, base_time)
                all_files.append(file)

                # simulate duplicate file ingestion
                if random.random() < 0.1:
                    duplicate_path = file.replace(".json", "_dup.json")
                    with open(file, "r") as src, open(duplicate_path, "w") as dst:
                        dst.write(src.read())

    print(f"Generated {len(all_files)} files")


# Rull all generators in one shot
def generate_all():
    base = BASE_PATH

    generate_products(f"{base}/products")
    generate_stores(f"{base}/stores")
    generate_customers(f"{base}/customers")
    generate_inventory(f"{base}/inventory")

    # POS generator from previous code
    # generate_data()
    generate_data(days=7, hours=12, files_per_hour=4, records_per_file=5000)



# Run generator
generate_all()
