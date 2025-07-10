import pandas as pd
import numpy as np
from faker import Faker
import datetime, random, os

# Paths
RAW = "data/raw/insurance_claims.csv"
OUT_DIR = "data/raw/simulated_stream_input"
os.makedirs(OUT_DIR, exist_ok=True)
OUT = os.path.join(OUT_DIR, "claims_200k.parquet")

# Load original
orig = pd.read_csv(RAW)

# Setup
fake = Faker()
TARGET = 200_000

# Realistic time window
end = datetime.datetime.now()
start = end - datetime.timedelta(days=30)
delta = (end - start).total_seconds()

rows = []
for _ in range(TARGET):
    sample = random.choice(orig.to_dict("records"))
    rows.append({
        "age": sample["age"],
        "months_as_customer": sample["months_as_customer"],
        "total_claim_amount": float(np.random.uniform(orig["total_claim_amount"].min(),
                                               orig["total_claim_amount"].max())),
        "fraud_reported": np.random.choice(["Y","N"], p=[0.1,0.9]),
        # Timestamp anywhere in the last 30 days
        "event_ts": (start + datetime.timedelta(seconds=random.uniform(0, delta))).isoformat(),
        "country": random.choice(["Germany","France","Italy","Spain","Poland"]),
        "currency": "EUR"
    })

df = pd.DataFrame(rows)
df.to_parquet(OUT, index=False)
print(f"✔️ Generated {len(df)} rows → {OUT}")
