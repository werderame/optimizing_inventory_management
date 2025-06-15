import os
import importlib
from modules import recipes, menu, database, purchase, fefo_func as ff, optimize as sol, analyze
import pandas as pd
import numpy as np
import warnings

# Import all modules dynamically
importlib.reload(recipes)
importlib.reload(menu)
importlib.reload(database)
importlib.reload(purchase)
importlib.reload(ff)
importlib.reload(sol)

# Suppress specific warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Set up output directory
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = os.path.join(base_dir, "Optimization_project", "data", "model_output")

# === SCRAPE RECIPES ===
#recipes.generate_hellofresh_bom() 

# === CREATE MENU ===
#menu.plan_menu(min_meals=950, max_meals=1050, weeks=6, start_date='2025-05-01') 

# === LOAD DB DATA ===
database.create_database()
database.create_tables()
database.load_data()
demand = database.create_demand_summary_view()

purchases = purchase.generate_purchase_list()
inventory = purchase.purchase_inventory(purchases, 
                                        first_treshold_sl = 9, second_treshold_sl = 21, 
                                        first_buffer = 1.005, second_buffer = 1.1, third_buffer = 1.5)
purchase.load_purchases(inventory)

# === FEFO ALLOCATION ===
d, i, w = ff.fefo_daily(demand, inventory, 0) # Perfect FEFO Ops
id, ii, iw = ff.fefo_daily(demand, inventory, 0.03)  # Imperfect FEFO Ops # 3% of inventory is replaced in the list randomly
print('💾 FEFO allocation complete! Output available as CSV.')   

# === OPTIMIZATION ===
print('Starting to Optimize Inventory')
#demand_suggested = sol.optimize_inventory(gap=0.05)
#sol.publish_solution(demand_suggested)
print('🧩 Optimization complete.')

# === RESULTS ===
errors = [[0, 'Perfect FEFO'], [0.03, 'Imperfect FEFO']]
results = analyze.results(demand, inventory, errors)

print(results)