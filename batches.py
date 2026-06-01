# import os
# import pandas as pd

# # 1. Define paths and your simulation cutoff window
# RAW_DATA_DIR = "C:/Users/eduar/OneDrive/Documentos/data_science/olist/"
# BATCH_OUTPUT_DIR = os.path.join(RAW_DATA_DIR, "batch_1_2017_q1")
# os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)

# START_DATE = "2017-01-01 00:00:00"
# END_DATE = "2017-03-31 23:59:59"

# print("--- Starting Time-Machine Split for Batch 1 (Q1 2017) ---")

# # ============================================================================
# # STEP 1: SPLIT THE MASTER ORDERS FILE & EXTRACT ACTIVE KEYS
# # ============================================================================
# orders_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_orders_dataset.csv"))

# # Filter orders strictly within our Q1 time window
# batch_orders = orders_df[
#     (orders_df["order_purchase_timestamp"] >= START_DATE)
#     & (orders_df["order_purchase_timestamp"] <= END_DATE)
# ]

# # Save the split orders file
# batch_orders.to_csv(
#     os.path.join(BATCH_OUTPUT_DIR, "olist_orders_dataset.csv"), index=False
# )

# # Isolate the explicit lists of IDs that belong to this batch
# active_order_ids = set(batch_orders["order_id"])
# active_customer_ids = set(batch_orders["customer_id"])

# print(f"Isolated {len(active_order_ids)} orders for this batch.")

# # ============================================================================
# # STEP 2: SPLIT THE DEPENDENT TABLES VIA ORDER_ID FILTERING
# # ============================================================================
# # These datasets depend directly on the orders placed in this batch
# dependent_order_files = {
#     "olist_order_items_dataset.csv": "order_id",
#     "olist_order_payments_dataset.csv": "order_id",
#     "olist_order_reviews_dataset.csv": "order_id",
# }

# # We will also keep track of what products and sellers are referenced in these items
# active_product_ids = set()
# active_seller_ids = set()

# for file_name, key_column in dependent_order_files.items():
#     df = pd.read_csv(os.path.join(RAW_DATA_DIR, file_name))

#     # Keep rows only if their order_id belongs to our active batch list
#     filtered_df = df[df[key_column].isin(active_order_ids)]
#     filtered_df.to_csv(os.path.join(BATCH_OUTPUT_DIR, file_name), index=False)
#     print(f"Filtered {file_name} -> Saved to batch folder.")

#     # Collect product and seller footprints from the items table
#     if file_name == "olist_order_items_dataset.csv":
#         active_product_ids = set(filtered_df["product_id"])
#         active_seller_ids = set(filtered_df["seller_id"])

# # ============================================================================
# # STEP 3: SPLIT THE ENTITY TABLES VIA RELATIONSHIP KEYS
# # ============================================================================
# # Slicing customers, products, and sellers so we only load entities active in Q1

# # Customers
# customers_df = pd.read_csv(
#     os.path.join(RAW_DATA_DIR, "olist_customers_dataset.csv")
# )
# filtered_customers = customers_df[
#     customers_df["customer_id"].isin(active_customer_ids)
# ]
# filtered_customers.to_csv(
#     os.path.join(BATCH_OUTPUT_DIR, "olist_customers_dataset.csv"), index=False
# )

# # Products
# products_df = pd.read_csv(
#     os.path.join(RAW_DATA_DIR, "olist_products_dataset.csv")
# )
# filtered_products = products_df[
#     products_df["product_id"].isin(active_product_ids)
# ]
# filtered_products.to_csv(
#     os.path.join(BATCH_OUTPUT_DIR, "olist_products_dataset.csv"), index=False
# )

# # Sellers
# sellers_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_sellers_dataset.csv"))
# filtered_sellers = sellers_df[sellers_df["seller_id"].isin(active_seller_ids)]
# filtered_sellers.to_csv(
#     os.path.join(BATCH_OUTPUT_DIR, "olist_sellers_dataset.csv"), index=False
# )

# print("\n--- Batch 1 Files Successfully Created! ---")


import os
import re
from datetime import datetime
import pandas as pd

# ============================================================================
# CONFIGURATION PANEL (Only change these variables for each batch run)
# ============================================================================
START_DATE = "2017-07-01 00:00:00"
END_DATE = "2017-09-30 23:59:59"

RAW_DATA_DIR = "C:/Users/eduar/OneDrive/Documentos/data_science/olist/"

# ============================================================================
# AUTOMATED DIRECTORY GENERATION ENGINE
# ============================================================================
# 1. Parse dates to calculate the target Year and Quarter
dt_start = datetime.strptime(START_DATE, "%Y-%m-%d %H:%i:%s" if "%i" in START_DATE else "%Y-%m-%d %H:%M:%S")
year = dt_start.strftime("%Y")
quarter = f"q{(dt_start.month - 1) // 3 + 1}"

# 2. Dynamically scan the path to find what batch number sequence we are on
existing_batches = [d for d in os.listdir(RAW_DATA_DIR) if os.path.isdir(os.path.join(RAW_DATA_DIR, d)) and d.startswith("batch_")]

batch_numbers = [0]
for folder in existing_batches:
    match = re.match(r"batch_(\d+)_", folder)
    if match:
        batch_numbers.append(int(match.group(1)))

# Next incremental batch integer
next_batch_num = max(batch_numbers) + 1

# 3. Assemble the final production path
folder_name = f"batch_{next_batch_num}_{year}_{quarter}"
BATCH_OUTPUT_DIR = os.path.join(RAW_DATA_DIR, folder_name)
os.makedirs(BATCH_OUTPUT_DIR, exist_ok=True)

print(f"--- Starting Time-Machine Split for {folder_name.upper()} ---")
print(f"Targeting Period: {START_DATE} to {END_DATE}\n")

# ============================================================================
# STEP 1: SPLIT THE MASTER ORDERS FILE & EXTRACT ACTIVE KEYS
# ============================================================================
orders_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_orders_dataset.csv"))

# Filter orders strictly within our dynamic time window
batch_orders = orders_df[
    (orders_df["order_purchase_timestamp"] >= START_DATE)
    & (orders_df["order_purchase_timestamp"] <= END_DATE)
]

# Save the split orders file
batch_orders.to_csv(
    os.path.join(BATCH_OUTPUT_DIR, "olist_orders_dataset.csv"), index=False
)

# Isolate the explicit lists of IDs that belong to this batch
active_order_ids = set(batch_orders["order_id"])
active_customer_ids = set(batch_orders["customer_id"])

print(f"Isolated {len(active_order_ids)} orders for this slice.")

# ============================================================================
# STEP 2: SPLIT THE DEPENDENT TABLES VIA ORDER_ID FILTERING
# ============================================================================
dependent_order_files = {
    "olist_order_items_dataset.csv": "order_id",
    "olist_order_payments_dataset.csv": "order_id",
    "olist_order_reviews_dataset.csv": "order_id",
}

active_product_ids = set()
active_seller_ids = set()

for file_name, key_column in dependent_order_files.items():
    df = pd.read_csv(os.path.join(RAW_DATA_DIR, file_name))

    # Keep rows only if their order_id belongs to our active batch list
    filtered_df = df[df[key_column].isin(active_order_ids)]
    filtered_df.to_csv(os.path.join(BATCH_OUTPUT_DIR, file_name), index=False)
    print(f"Filtered {file_name} -> Saved to folder.")

    # Collect product and seller footprints from the items table
    if file_name == "olist_order_items_dataset.csv":
        active_product_ids = set(filtered_df["product_id"])
        active_seller_ids = set(filtered_df["seller_id"])

# ============================================================================
# STEP 3: SPLIT THE ENTITY TABLES VIA RELATIONSHIP KEYS
# ============================================================================
# Customers
customers_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_customers_dataset.csv"))
filtered_customers = customers_df[customers_df["customer_id"].isin(active_customer_ids)]
filtered_customers.to_csv(os.path.join(BATCH_OUTPUT_DIR, "olist_customers_dataset.csv"), index=False)

# Products
products_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_products_dataset.csv"))
filtered_products = products_df[products_df["product_id"].isin(active_product_ids)]
filtered_products.to_csv(os.path.join(BATCH_OUTPUT_DIR, "olist_products_dataset.csv"), index=False)

# Sellers
sellers_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_sellers_dataset.csv"))
filtered_sellers = sellers_df[sellers_df["seller_id"].isin(active_seller_ids)]
filtered_sellers.to_csv(os.path.join(BATCH_OUTPUT_DIR, "olist_sellers_dataset.csv"), index=False)

print(f"\n--- {folder_name.upper()} Files Successfully Created! ---")