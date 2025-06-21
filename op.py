import os
import importlib
from modules import recipes, menu, database, purchase, fefo_func as ff, optimize as sol, analyze
import pandas as pd
import warnings
importlib.reload(recipes), importlib.reload(menu), importlib.reload(database), importlib.reload(purchase), importlib.reload(ff), importlib.reload(sol)

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Set up paths
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = os.path.join(base_dir, "Optimization_project", "data", "model_output")

# === SCRAPE RECIPES & CREATE RECIPES ===
#recipes.generate_hellofresh_bom() 
menu.plan_menu(min_meals=950, max_meals=1050, weeks=6, start_date='2025-05-01') 

# === LOAD DB DATA ===
database.create_database()
database.create_tables()
database.load_data()
demand = database.create_demand_summary_view()

# === PURCHASES ===
purchases = purchase.generate_purchase_list()
inventory = purchase.purchase_inventory(purchases)
purchase.load_purchases(inventory)

# === FEFO ALLOCATION ===
e_1 = 0.00  # Perfect FEFO
e_2 = 0.03  # Imperfect FEFO
d, i, w = ff.fefo_daily(demand, inventory, e_1, output_names=['d', 'i', 'w']) # Perfect FEFO Ops
id, ii, iw = ff.fefo_daily(demand, inventory, e_2, output_names=['id', 'ii', 'iw'])  # Imperfect FEFO Ops # 3% of inventory is replaced in the list randomly
print('💾 FEFO allocation complete! Output available as CSV.')   

# === OPTIMIZATION ===
print('Starting to Optimize Inventory')
demand_suggested = sol.optimize_inventory(gap=0.05)
sol.publish_solution(demand_suggested)

# === RESULTS ===
r1 = analyze.fefo_results('d', 'w', 'Perfect FEFO', e_1)
r2 = analyze.fefo_results('id', 'iw', 'Imperfect FEFO', e_2)
r3 = analyze.opt_results('d', 'w')
print('\n🧩 Results of Optimization:')
print(pd.concat([r1, r2, r3], ignore_index=True))