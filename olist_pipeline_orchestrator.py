import os
import re
import time
from datetime import datetime
import mysql.connector
import pandas as pd

# ============================================================================
# 1. CONFIGURATION & DATABASE CREDENTIALS
# ============================================================================
RAW_DATA_DIR = "C:/Users/eduar/OneDrive/Documentos/data_science/olist/"
T_MINUTES = 1  # Time interval between automated updates

DB_CONFIG = {
    "host": "localhost",
    "user": "root",          
    "password": "qpalzm2011100%" 
}

# RESET: Complete timeline from the very first batch to the last data footprint
FUTURE_BATCHES = [
    {"start": "2017-01-01 00:00:00", "end": "2017-03-31 23:59:59"}, # 2017 Q1 (Batch 1)
    {"start": "2017-04-01 00:00:00", "end": "2017-06-30 23:59:59"}, # 2017 Q2 (Batch 2)
    {"start": "2017-07-01 00:00:00", "end": "2017-09-30 23:59:59"}, # 2017 Q3 (Batch 3)
    {"start": "2017-10-01 00:00:00", "end": "2017-12-31 23:59:59"}, # 2017 Q4 (Batch 4)
    {"start": "2018-01-01 00:00:00", "end": "2018-03-31 23:59:59"}, # 2018 Q1 (Batch 5)
    {"start": "2018-04-01 00:00:00", "end": "2018-06-30 23:59:59"}, # 2018 Q2 (Batch 6)
    {"start": "2018-07-01 00:00:00", "end": "2018-08-31 23:59:59"}, # 2018 Q3 (Batch 7)
]

# ============================================================================
# 2. HELPER FUNCTION: SLICE RAW DATA (Python Engine)
# ============================================================================
def extract_and_slice_batch(start_date, end_date):
    # Parse dates to calculate Target Year and Quarter for folder name
    dt_start = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    year = dt_start.strftime("%Y")
    quarter = f"q{(dt_start.month - 1) // 3 + 1}"
    
    # Calculate next incremental batch integer dynamically
    existing_batches = [d for d in os.listdir(RAW_DATA_DIR) if os.path.isdir(os.path.join(RAW_DATA_DIR, d)) and d.startswith("batch_")]
    batch_numbers = [0]
    for folder in existing_batches:
        match = re.match(r"batch_(\d+)_", folder)
        if match: batch_numbers.append(int(match.group(1)))
    
    next_batch_num = max(batch_numbers) + 1
    folder_name = f"batch_{next_batch_num}_{year}_{quarter}"
    batch_output_dir = os.path.join(RAW_DATA_DIR, folder_name)
    os.makedirs(batch_output_dir, exist_ok=True)
    
    print(f"\n==================================================================")
    print(f"🚀 EXECUTING PIPELINE FOR: {folder_name.upper()}")
    print(f"Time Window: {start_date} to {end_date}")
    print(f"==================================================================")
    
    # Slice Orders
    orders_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_orders_dataset.csv"))
    batch_orders = orders_df[(orders_df["order_purchase_timestamp"] >= start_date) & (orders_df["order_purchase_timestamp"] <= end_date)]
    batch_orders.to_csv(os.path.join(batch_output_dir, "olist_orders_dataset.csv"), index=False)
    
    active_order_ids = set(batch_orders["order_id"])
    active_customer_ids = set(batch_orders["customer_id"])
    
    # Slice Dependent Order Tables
    dependent_files = ["olist_order_items_dataset.csv", "olist_order_payments_dataset.csv", "olist_order_reviews_dataset.csv"]
    active_product_ids, active_seller_ids = set(), set()
    
    for file_name in dependent_files:
        df = pd.read_csv(os.path.join(RAW_DATA_DIR, file_name))
        filtered_df = df[df["order_id"].isin(active_order_ids)]
        filtered_df.to_csv(os.path.join(batch_output_dir, file_name), index=False)
        if file_name == "olist_order_items_dataset.csv":
            active_product_ids = set(filtered_df["product_id"])
            active_seller_ids = set(filtered_df["seller_id"])
            
    # Slice Entities
    for file, keys, col in [("olist_customers_dataset.csv", active_customer_ids, "customer_id"),
                            ("olist_products_dataset.csv", active_product_ids, "product_id"),
                            ("olist_sellers_dataset.csv", active_seller_ids, "seller_id")]:
        df = pd.read_csv(os.path.join(RAW_DATA_DIR, file))
        df[df[col].isin(keys)].to_csv(os.path.join(batch_output_dir, file), index=False)
        
    print(f"🔹 Step 1 Complete: {len(active_order_ids)} raw rows sliced into local directory.")
    return batch_output_dir


# ============================================================================
# 3. HELPER FUNCTION: EXECUTE SQL AUTOMATION (Database Engine)
# ============================================================================
def execute_database_etl(batch_dir):
    conn = mysql.connector.connect(**DB_CONFIG, allow_local_infile=True)
    cursor = conn.cursor()
    safe_path = batch_dir.replace("\\", "/")
    
    try:
        # A. CYCLING STAGING LAYER
        print("🔹 Step 2: Cycling Staging Environment (Wipe & Replace)...")
        tables = ['stg_orders', 'stg_order_items', 'stg_payments', 'stg_reviews', 'stg_customers', 'stg_products', 'stg_sellers']
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE olist_staging.{table};")
            
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_orders_dataset.csv'
            INTO TABLE olist_staging.stg_orders CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_order_items_dataset.csv'
            INTO TABLE olist_staging.stg_order_items CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_order_payments_dataset.csv'
            INTO TABLE olist_staging.stg_payments CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_order_reviews_dataset.csv'
            INTO TABLE olist_staging.stg_reviews CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_customers_dataset.csv'
            INTO TABLE olist_staging.stg_customers CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_products_dataset.csv'
            INTO TABLE olist_staging.stg_products CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)
        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{safe_path}/olist_sellers_dataset.csv'
            INTO TABLE olist_staging.stg_sellers CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
        """)

        # B. MIGRATING TO WAREHOUSE
        print("🔹 Step 3: Executing Warehouse Appends & Dimension Upserts...")
        
        cursor.execute("""
            INSERT INTO olist_warehouse.dim_customers (customer_unique_key, customer_zip_code_prefix, customer_city, customer_state)
            SELECT customer_unique_id, MAX(customer_zip_code_prefix), MAX(UPPER(TRIM(customer_city))), MAX(UPPER(TRIM(customer_state)))
            FROM olist_staging.stg_customers GROUP BY customer_unique_id
            ON DUPLICATE KEY UPDATE customer_zip_code_prefix = VALUES(customer_zip_code_prefix), customer_city = VALUES(customer_city), customer_state = VALUES(customer_state);
        """)
        
        cursor.execute("""
            INSERT INTO olist_warehouse.dim_products (product_key, product_category_name_english, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            SELECT p.product_id, COALESCE(t.product_category_name_english, NULLIF(p.product_category_name, ''), 'unknown'),
                CAST(CAST(NULLIF(p.product_weight_g, '') AS DECIMAL(10,2)) AS UNSIGNED), CAST(CAST(NULLIF(p.product_length_cm, '') AS DECIMAL(10,2)) AS UNSIGNED),
                CAST(CAST(NULLIF(p.product_height_cm, '') AS DECIMAL(10,2)) AS UNSIGNED), CAST(CAST(NULLIF(p.product_width_cm, '') AS DECIMAL(10,2)) AS UNSIGNED)
            FROM olist_staging.stg_products p LEFT JOIN olist_staging.stg_product_category_translation t ON p.product_category_name = t.product_category_name
            ON DUPLICATE KEY UPDATE product_category_name_english = VALUES(product_category_name_english), product_weight_g = VALUES(product_weight_g), product_length_cm = VALUES(product_length_cm), product_height_cm = VALUES(product_height_cm), product_width_cm = VALUES(product_width_cm);
        """)
        
        cursor.execute("""
            INSERT INTO olist_warehouse.dim_sellers (seller_key, seller_zip_code_prefix, seller_city, seller_state)
            SELECT seller_id, seller_zip_code_prefix, UPPER(TRIM(seller_city)), UPPER(TRIM(seller_state)) FROM olist_staging.stg_sellers
            ON DUPLICATE KEY UPDATE seller_city = VALUES(seller_city);
        """)
        
        cursor.execute("""
            INSERT INTO olist_warehouse.dim_order_reviews (review_key, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
            SELECT review_id, order_id, review_score, NULLIF(review_comment_title, ''), NULLIF(review_comment_message, ''), review_creation_date, review_answer_timestamp
            FROM olist_staging.stg_reviews ON DUPLICATE KEY UPDATE review_score = VALUES(review_score);
        """)
        
        cursor.execute("""
            INSERT IGNORE INTO olist_warehouse.fact_order_items
            SELECT 
                CONCAT(oi.order_id, '-', oi.order_item_id), oi.order_id, c.customer_unique_id, oi.product_id, oi.seller_id,
                CONCAT(c.customer_state, '_', YEAR(STR_TO_DATE(o.order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'))), o.order_status,
                STR_TO_DATE(NULLIF(o.order_purchase_timestamp, ''), '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(NULLIF(o.order_approved_at, ''), '%Y-%m-%d %H:%i:%s'),
                STR_TO_DATE(NULLIF(o.order_delivered_carrier_date, ''), '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(NULLIF(o.order_delivered_customer_date, ''), '%Y-%m-%d %H:%i:%s'),
                STR_TO_DATE(NULLIF(o.order_estimated_delivery_date, ''), '%Y-%m-%d %H:%i:%s'), oi.price, oi.freight_value, (oi.price + oi.freight_value),
                DATEDIFF(STR_TO_DATE(o.order_delivered_customer_date, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(o.order_purchase_timestamp, '%Y-%m-%d %H:%i:%s')),
                DATEDIFF(STR_TO_DATE(o.order_estimated_delivery_date, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE(o.order_delivered_customer_date, '%Y-%m-%d %H:%i:%s'))
            FROM olist_staging.stg_order_items oi
            INNER JOIN olist_staging.stg_orders o ON oi.order_id = o.order_id
            INNER JOIN olist_staging.stg_customers c ON o.customer_id = c.customer_id;
        """)
        
        cursor.execute("""
            INSERT IGNORE INTO olist_warehouse.fact_order_payments
            SELECT CONCAT(order_id, '-', payment_sequential, '-', payment_type), order_id, payment_sequential, payment_type, payment_installments, payment_value
            FROM olist_staging.stg_payments;
        """)
        
        conn.commit()
        
        # C. PROFILING THE CURRENT WAREHOUSE STATE
        cursor.execute("SELECT COUNT(*), MIN(order_purchase_timestamp), MAX(order_purchase_timestamp) FROM olist_warehouse.fact_order_items;")
        rows, start_pt, end_pt = cursor.fetchone()
        
        print(f"✨ SUCCESS: Warehouse completely synchronized!")
        print(f"📊 Current Warehouse Footprint: {rows} total items rows.")
        print(f"📅 Current Date Horizon: {start_pt} to {end_pt}")
        
    except mysql.connector.Error as err:
        print(f"❌ DATABASE ERROR: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# ============================================================================
# 4. ORCHESTRATION EVENT LOOP
# ============================================================================
if __name__ == "__main__":
    print(f"Initializing Data Automation Loop. Interval set to {T_MINUTES} minutes.")
    print("Open Power BI and prepare to hit refresh when alerted!\n")
    
    for i, timeframe in enumerate(FUTURE_BATCHES):
        # 1. Run Python File Slicer
        target_dir = extract_and_slice_batch(timeframe["start"], timeframe["end"])
        
        # 2. Run Database Loading and Transformation Pipeline
        execute_database_etl(target_dir)
        
        # 3. Notification to trigger user interface refresh
        print(f"\n🔔 PIPELINE PAUSED: Go check Power BI and hit REFRESH right now!")
        
        if i < len(FUTURE_BATCHES) - 1:
            print(f"Waiting {T_MINUTES} minutes before processing next batch...")
            time.sleep(T_MINUTES * 60)
            
    print("\n🏁 ALL HISTORICAL BATCHES SUCCESSFULLY STREAMED INTO THE PRODUCTION WAREHOUSE!")