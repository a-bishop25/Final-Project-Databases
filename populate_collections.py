from pymongo import MongoClient
import pandas as pd
import os
# Replace with your actual MongoDB Atlas connection string
MONGO_URI = "mongodb+srv://abishop25_db_user:<password>@cluster0.szrzgd6.mongodb.net/?appName=Cluster0"

# Connect to the cluster
client = MongoClient(MONGO_URI)

# Access your database
db = client["municipal_bonds"]

# ===========================================================
# 2. File paths (adjust if needed)
# ===========================================================

FILES = {
    "bonds": "bonds.csv",
    "trades": "trades.csv",
    "issuers": "issuers.csv",
    "bond_purposes": "bond_purposes.csv",
    "macro_economic_data": "economic_indicators.csv",
    "credit_ratings": "credit_ratings.csv",
    "economic_indicators": "economic_indicators.csv"
}

# ===========================================================
# 3. Helper function to load and insert
# ===========================================================

def load_and_insert(collection_name, file_path):
    print(f"Loading {file_path} into {collection_name}...")
    df = pd.read_csv(file_path)

    # Convert date columns to datetime if present
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert numeric columns to float
    for col in df.select_dtypes(include='object').columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            pass  # ignore non-numeric columns

    # Drop existing collection to avoid duplicates
    db[collection_name].drop()

    # Insert into MongoDB
    records = df.to_dict('records')
    if records:
        db[collection_name].insert_many(records)
    print(f"Inserted {len(records)} records into {collection_name}.\n")

# ===========================================================
# 4. Populate all collections
# ===========================================================

for collection, file_path in FILES.items():
    load_and_insert(collection, file_path)

print("All collections populated successfully!")
