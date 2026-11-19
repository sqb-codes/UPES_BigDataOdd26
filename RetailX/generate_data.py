import csv
import json
import random
from datetime import datetime, timedelta

random.seed(42)

# -----------------------------
# 1. Generate Orders (CSV)
# -----------------------------

cities = ["Delhi", "Noida", "Gurgaon", "Mumbai", "Bangalore"]
categories = ["Electronics", "Fashion", "Grocery", "Books"]
statuses = ["Completed", "Pending", "Cancelled"]

orders = []

for i in range(1, 101):
    orders.append({
        "order_id": 1000 + i,
        "customer_id": f"C{random.randint(1, 30):03d}",
        "city": random.choice(cities),
        "category": random.choice(categories),
        "amount": random.randint(500, 30000),
        "status": random.choice(statuses),
        "order_date": (
            datetime(2026, 11, 1) +
            timedelta(days=random.randint(0, 16))
        ).strftime("%Y-%m-%d")
    })

# Intentionally introduce data-quality problems
orders[5]["amount"] = -500
orders[10]["customer_id"] = ""
orders.append(orders[0].copy())  # Duplicate order

with open("source-data/orders.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)

# -----------------------------
# 2. Generate Customers (JSON)
# -----------------------------

customers = []

for i in range(1, 31):
    customers.append({
        "customer_id": f"C{i:03d}",
        "name": f"Customer {i}",
        "email": f"customer{i}@example.com",
        "location": {
            "city": random.choice(cities),
            "country": "India"
        },
        "membership": random.choice(
            ["Silver", "Gold", "Platinum"]
        )
    })

# Introduce a missing email
customers[3]["email"] = None

with open("source-data/customers.json", "w") as f:
    json.dump(customers, f, indent=4)

# -----------------------------
# 3. Generate Application Logs
# -----------------------------

events = [
    "USER_LOGIN",
    "PRODUCT_VIEW",
    "ADD_TO_CART",
    "PAYMENT_SUCCESS",
    "PAYMENT_FAILED"
]

with open("source-data/application.log", "w") as f:
    for i in range(1, 51):
        event = random.choice(events)
        timestamp = (
            datetime(2026, 11, 18, 10, 0) +
            timedelta(seconds=i * 10)
        )

        f.write(
            f"{timestamp} | USER_{random.randint(1,30)} "
            f"| {event}\n"
        )

print("RetailX source datasets generated!")
print("1. orders.csv")
print("2. customers.json")
print("3. application.log")