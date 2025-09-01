import os
import pandas as pd
from datetime import datetime

CLEAN_DATA_DIR = "data/clean_data"
ENRICHED_DATA_DIR = "data/enriched_data"


def enrich_data(date: datetime):
    clients_path = f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}/{date.day}_clean.csv"
    products_path = f"{CLEAN_DATA_DIR}/products/{date.year}/{date.month}/{date.day}_clean.csv"
    orders_path = f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}/{date.day}_clean.csv"
    
    if not all(os.path.exists(p) for p in [clients_path, products_path, orders_path]):
        print("Données nettoyées manquantes pour l’enrichissement.")
        return
    
    clients = pd.read_csv(clients_path)
    products = pd.read_csv(products_path)
    orders = pd.read_csv(orders_path)
    
    
    clients = clients[["customer_id", "firstname", "lastname", "email"]]
    products = products[["product_id", "product_name", "stock"]]
    orders = orders[["order_id", "order_date", "customer_id", "product_id", "quantity", "price"]]
    
    
    enriched = orders.merge(clients, on="customer_id", how="left")
   
    enriched = enriched.merge(products, on="product_id", how="left")
    
    # calcul du prix total
    enriched["total_price"] = enriched["quantity"] * enriched["price"]
    
    enrich_dir = f"{ENRICHED_DATA_DIR}/{date.year}/{date.month}"
    os.makedirs(enrich_dir, exist_ok=True)
    enrich_path = os.path.join(enrich_dir, f"{date.day}_enriched.csv")
    enriched.to_csv(enrich_path, index=False)
    
    print(f"Données enrichies enregistrées dans {enrich_path}")
    return enrich_path

if __name__ == "__main__":
    enrich_data(datetime.strptime("2024-05-10", "%Y-%m-%d"))