import os
import importlib
from modules import recipes, menu, database, purchase, fefo_func as ff, optimize as sol
import pandas as pd
import numpy as np

importlib.reload(recipes)
importlib.reload(menu)
importlib.reload(database)
importlib.reload(purchase)
importlib.reload(ff)
importlib.reload(sol)


base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = os.path.join(base_dir, "Optimization_project", "data", "model_output")

#recipes.generate_hellofresh_bom() 

#menu.plan_menu(min_meals=950, max_meals=1050, weeks=6, start_date='2025-05-01') 

database.create_database()
database.create_tables()
database.load_data()
demand = database.create_demand_summary_view()

purchases = purchase.generate_purchase_list()
inventory = purchase.purchase_inventory(purchases, 
                                        first_treshold_sl = 9, second_treshold_sl = 21, 
                                        first_buffer = 1.005, second_buffer = 1.1, third_buffer = 1.5)
purchase.load_purchases(inventory)

# demand and inventory are now ready and operations can process them

# Perfect FEFO Ops
d, i, w = ff.fefo_daily(demand, inventory, 0)
output_files = {'p_output_demand': d, 'p_output_inventory': i, 'p_output_waste': w}
for name, df in output_files.items():
    df.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)

# Imperfect FEFO Ops
id, ii, iw = ff.fefo_daily(demand, inventory, 0.03)  # 3% of inventory is replaced in the list randomly
output_files = {'i_output_demand': id, 'i_output_inventory': ii, 'i_output_waste': iw}
for name, df in output_files.items():
    df.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)

print('FEFO allocation complete! Demand, Inventory and Waste Output datasets available in the folder model_output.')   

print('Starting to Optimize Inventory')
demand_suggested = sol.optimize_inventory(gap=0.05)
sol.publish_solution(demand_suggested)



# Imperfect FEFO Ops

errors = np.linspace(0, 0.15, 11)
results_list = []
for e in errors:
    id, ii, iw = ff.fefo_daily(demand, inventory, e)  
    
    # vars
    i_waste = iw['expired_quantity'].sum()
    i_fulfilled = id['fulfilled_demand_q'].sum()
    i_shortages = id.groupby(['art_code', 'demand_date'])['remaining_demand_q'].last().sum()
    total_demand = id[['art_code', 'demand_date', 'nominal_demand_q']].drop_duplicates()['nominal_demand_q'].sum()

    # results
    print(f'--- Imperfect Daily FEFO with {e * 100:.1f}% randomness')
    print(f'- Units to waste: {i_waste}')
    print(f'- Shortages: {i_shortages}')
    print(f'- Waste + Shortages: {i_shortages + i_waste}')
    print(f'- Percentage of Waste + Shortages: {(i_waste + i_shortages) / total_demand * 100:.2f}%\n')
    print(f'- Met demand: {i_fulfilled}')
    print(f'- Met demand: {i_fulfilled / total_demand * 100:.2f}%')
    print('-----------------------')
    
    results_list.append({
    'randomness_pct': e * 100,
    'waste_units': i_waste,
    'shortage_units': i_shortages,
    'total_loss_units': i_waste + i_shortages,
    'loss_pct': (i_waste + i_shortages) / total_demand * 100,
    'fulfilled_units': i_fulfilled,
    'fulfilled_pct': i_fulfilled / total_demand * 100,
    'total_demand': total_demand
})
results_df = pd.DataFrame(results_list)
print(results_df)