import os
import pandas as pd
from datetime import datetime

RAW_DATA_DIR = "data/raw_data"
CLEAN_DATA_DIR = "data/clean_data"


def clean_clients(date: datetime):
    raw_path = os.path.join(RAW_DATA_DIR, f"clients/{date.year}/{date.month}/{date.day}.csv")
    if not os.path.exists(raw_path):
        print("Fichier brut clients introuvable.")
        return
    
    df = pd.read_csv(raw_path)
    # Suppression doublons et valeurs manquantes
    df = df.drop_duplicates(subset=["customer_id"])
    df = df.dropna(subset=["customer_id", "firstname", "lastname", "email"])
    df["firstname"] = df["firstname"].str.strip()
    df["lastname"] = df["lastname"].str.strip()
    df["email"] = df["email"].str.lower()
    
    clean_dir = f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}"
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, f"{date.day}_clean.csv")
    df.to_csv(clean_path, index=False)
    
    print(f"Clients nettoyés enregistrés dans {clean_path}")
    return clean_path


def clean_products(date: datetime):
    raw_path = os.path.join(RAW_DATA_DIR, f"products/{date.year}/{date.month}/{date.day}.csv")
    if not os.path.exists(raw_path):
        print("Fichier brut produits introuvable.")
        return
    
    df = pd.read_csv(raw_path)
    df = df.drop_duplicates(subset=["product_id"])
    df = df.dropna(subset=["product_id", "product_name", "stock"])
    df["product_name"] = df["product_name"].str.strip()
    df["stock"] = pd.to_numeric(df["stock"], errors="coerce")
    df = df.dropna(subset=["stock"])
    
    clean_dir = f"{CLEAN_DATA_DIR}/products/{date.year}/{date.month}"
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, f"{date.day}_clean.csv")
    df.to_csv(clean_path, index=False)
    
    print(f"Produits nettoyés enregistrés dans {clean_path}")
    return clean_path


def clean_orders(date: datetime):
    raw_path = os.path.join(RAW_DATA_DIR, f"orders/{date.year}/{date.month}/{date.day}.csv")
    if not os.path.exists(raw_path):
        print("Fichier brut commandes introuvable.")
        return
    
    df = pd.read_csv(raw_path)
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "customer_id", "product_id", "quantity", "price"])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["quantity", "price"])
    
    clean_dir = f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}"
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, f"{date.day}_clean.csv")
    df.to_csv(clean_path, index=False)
    
    print(f"Commandes nettoyées enregistrées dans {clean_path}")
    return clean_path


if __name__ == "__main__":
    clean_clients(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    clean_products(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    clean_orders(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    clean_orders(datetime.strptime("2024-05-03", "%Y-%m-%d"))