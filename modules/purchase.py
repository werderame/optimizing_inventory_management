# purchase.py

import os
import pandas as pd
import psycopg2 as pg
import psycopg2.extras
import numpy as np
from datetime import timedelta
from . import db_config as db
import random

# === 0. DB Configuration and Directories ===

DB_NAME=db.DB_NAME
DB_USER=db.DB_USER
DB_PASSWORD=db.DB_PASSWORD
DB_HOST=db.DB_HOST
DB_PORT=db.DB_PORT


base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.join(base_dir, "data", "db_ready")

random.seed(42) # reproduce results
np.random.seed(42)

def generate_purchase_list():
    
    # === 1. Query Ingredient Demand from DB ===

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    dict_cur.execute("SELECT * FROM demand_summary ")
    result = dict_cur.fetchall()
    demand = []
    for row in result: 
        demand.append(dict(row))
    dict_cur.close()
    conn.close()
    demand = pd.DataFrame(demand)

    # === 2. Enrich with Master Data ===

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    dict_cur.execute("SELECT DISTINCT art_code, art_category, shelf_life FROM article ")
    result = dict_cur.fetchall()
    master_data = []
    for row in result: 
        master_data.append(dict(row))
    dict_cur.close()
    conn.close()
    master_data = pd.DataFrame(master_data)
    master_data = master_data.drop_duplicates(subset=['art_code'], keep='first')

    demand = pd.merge(demand, master_data, on='art_code', how='left') # Merge demand with master data

    # === 3. Aggregate Demand Lines based on Shelf Life ===

    # we consider quantities with a close date as a single non-fragmented quantity, 
    # replicating the logic of how purchase orders work.
    
    """ 3.1 # Fill NA MLOR values """
    demand['shelf_life'] = demand['shelf_life'].fillna(demand.groupby('art_category')['shelf_life'].transform('mean'))
    demand['shelf_life'] = demand['shelf_life'].fillna(9)
    demand['shelf_life'] = demand['shelf_life'].clip(lower=9) # ensure no demand is lower than..
    demand['shelf_life'] = demand['shelf_life'].astype('int')

    """ 3.2 Aggregate the demand lines """
    demand = demand.sort_values(by=['art_code', 'demand_date'])  # Sort the DataFrame
    demand['demand_date'] = pd.to_datetime(demand['demand_date'])  # Ensure demand_date is a date
    def aggregate_window(demand, fresh_date_window=1, semi_fresh_date_window=2, non_fresh_date_window=7):
        agg_demand = []
        last_row = None

        for i, row in demand.iterrows():
            
            # Determine the current window based on shelf life
            if row['shelf_life'] <= 9:
                current_window = fresh_date_window
            elif row['shelf_life'] <= 12:
                current_window = semi_fresh_date_window
            else:
                current_window = non_fresh_date_window
            

            # If last_row exists and should be aggregated
            if last_row is not None and row.art_code == last_row.art_code and (row.demand_date - last_row.demand_date).days < current_window:
                last_row.art_demand += row.art_demand  # Aggregate demand quantity
            
            else:
                if last_row is not None:
                    agg_demand.append(last_row)  # Store the last aggregated row
                last_row = row.copy()  # Copy new row for tracking

        # Append the last processed row
        if last_row is not None:
            agg_demand.append(last_row)

        return pd.DataFrame(agg_demand)

    agg_demand = aggregate_window(demand) # Apply Aggregation

    return agg_demand

import numpy as np
import pandas as pd
from datetime import timedelta

def purchase_inventory(agg_demand, first_treshold_sl=9, second_treshold_sl=21, first_buffer=1.005, second_buffer=1.1, third_buffer=1.5):
    
    # === 4. "Purchase", i.e. Create Inventory Quantities ===
    
    """ 4.1 Create the inventory quantities based on the aggregated demand and shelf life. """
    agg_demand['quantity'] = 0
    for i, row in agg_demand.iterrows():
        if row['shelf_life'] <= first_treshold_sl:
            agg_demand.loc[i, 'quantity'] = row['art_demand'] * np.random.uniform(1.0, first_buffer)
        elif row['shelf_life'] <= second_treshold_sl:
            agg_demand.loc[i, 'quantity'] = row['art_demand'] * np.random.uniform(1.0, second_buffer)
        else:
            agg_demand.loc[i, 'quantity'] = row['art_demand'] * np.random.uniform(1.0, third_buffer)

    agg_demand['quantity'] = agg_demand['quantity'].astype(int)
    agg_demand['quantity'] = agg_demand['quantity'].apply(lambda x: max(10, x))  # at least 10 pcs at inventory
    agg_demand.reset_index(drop=True, inplace=True)
    agg_demand['batch_id'] = agg_demand.index + 1

    """ 4.2 Assign an expiration date to the purchased inventory """
    def assign_expiration(row):
        sl = row['shelf_life']
        if sl <= first_treshold_sl:
            min_days, max_days = 2, min(sl, 7)
        elif sl <= second_treshold_sl:
            min_days, max_days = 4, sl
        else:
            min_days, max_days = 15, sl
        min_days = min(min_days, sl)
        max_days = max(min_days + 1, sl)
        offset_days = np.random.randint(min_days, max_days + 1)
        return row['demand_date'] + timedelta(days=offset_days)

    agg_demand['expiration_date'] = agg_demand.apply(assign_expiration, axis=1)

    agg_demand = agg_demand[['art_code', 'quantity', 'expiration_date', 'batch_id']]

    """ 4.3 "Palletize" inventory, i.e. fragment the inventory into pallets of max 1_000 units."""
    max_pallet = 25 * 4 * 10  # 1_000 units per pallet
    pal_inventory = agg_demand.copy()

    palletized = []
    for i, row in pal_inventory.iterrows():
        while row.quantity > max_pallet:
            temp_row = row.copy()
            temp_row['quantity'] = max_pallet
            palletized.append(temp_row)
            row['quantity'] -= max_pallet
        palletized.append(row)

    palletized = pd.DataFrame(palletized).reset_index(drop=True)
    palletized['inv_id'] = palletized.index + 1
    inventory = palletized[['inv_id', 'art_code', 'quantity', 'expiration_date', 'batch_id']]
    return inventory

def load_purchases(inventory):
    
    # === 5. Save Inventory to CSV and Load into DB ===
    
    """ 5.1 Save CSV """
    inventory.to_csv(os.path.join(db_dir, "inventory_table.csv"), index=False)

    """ 5.2 Load into DB """
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()

        # Load CSVs
        csv_path = os.path.join(os.getcwd(), db_dir, "inventory_table.csv")
        with open(csv_path, 'r', encoding='utf-8') as f:
            cur.copy_expert(f"COPY inventory (inv_id,art_code,quantity,expiration_date,batch_id) FROM STDIN WITH CSV HEADER DELIMITER ','", f)
        print(f"✅ Loaded data into inventory.")

        conn.commit()
        cur.close()
        conn.close()
        print("🎯 All data loaded successfully.")

    except Exception as e:
        print(f"❌ Error loading data in inventory: {e}")

# === Run Setup ===
if __name__ == "__main__":
    generate_purchase_list()
    purchase_inventory()
    load_purchases()


