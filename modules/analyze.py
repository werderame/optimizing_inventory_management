from modules import fefo_func as ff
import os
import pandas as pd

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = os.path.join(base_dir, "data", "model_output")




def fefo_results(demand_input, waste_input, name=None, e=None):
    results_list = [] 

    demand_development = pd.read_csv(os.path.join(output_dir, f'{demand_input}.csv'))
    waste_development = pd.read_csv(os.path.join(output_dir, f'{waste_input}.csv'))
    
    # vars
    i_waste = waste_development['expired_quantity'].sum()
    i_fulfilled = demand_development['fulfilled_demand_q'].sum()
    i_shortages = demand_development.groupby(['art_code', 'demand_date'])['remaining_demand_q'].last().sum()
    total_demand = demand_development[['art_code', 'demand_date', 'nominal_demand_q']].drop_duplicates()['nominal_demand_q'].sum()
    
    results_list.append({
    'model': name if name else 'FEFO'
    , 'error_pct': e * 100
    , 'waste_pcs': i_waste
    , 'shortage_pcs': i_shortages
    , 'loss_pct': round((i_waste + i_shortages) / total_demand * 100, 2)
    , 'fulfilled_d': i_fulfilled
    , 'fulfilled_pct': round(i_fulfilled / total_demand * 100, 2)
})
    results_df = pd.DataFrame(results_list)
    return results_df


def opt_results(demand_input, waste_input):

    demand_development = pd.read_csv(os.path.join(output_dir, f'{demand_input}.csv'))
    shortages_start = demand_development.groupby(['art_code', 'demand_date'])['remaining_demand_q'].last().sum()
    fulfilled_start = demand_development['fulfilled_demand_q'].sum()

    total_demand = demand_development[['art_code', 'demand_date', 'nominal_demand_q']].drop_duplicates()['nominal_demand_q'].sum()

    waste_development = pd.read_csv(os.path.join(output_dir, f'{waste_input}.csv'))[['inv_id', 'expired_quantity']]

    optimal_fefo_w = pd.read_csv(os.path.join(output_dir,'output_add_inventory.csv'))[['inv_id', 'qty_used']]
    optimal_fefo_w = optimal_fefo_w.groupby(["inv_id"]).agg(total_qty_used=("qty_used", "sum")).reset_index() # Additional usage
    optimal_fefo_w['total_qty_used'] = optimal_fefo_w['total_qty_used'].astype(int)

    waste_pro = pd.merge(waste_development, optimal_fefo_w, on='inv_id', how='right').fillna(0)
    waste_pro = waste_pro.rename(columns={'total_qty_used': 'opt_fefo_usage'})
    waste_pro = waste_pro.rename(columns={'expired_quantity': 'per_fefo_w'})
    waste_pro['opt_fefo_w'] = waste_pro['per_fefo_w'] - waste_pro['opt_fefo_usage'].astype(int)
    # enforce only positive waste (negative would correspond to units in addition to customer demand)
    waste_pro['opt_fefo_w'] = waste_pro['opt_fefo_w'].clip(lower=0) 

    o_w_total = sum(waste_pro['opt_fefo_w'])

    additional_o_inventory = pd.read_csv('./data/model_output/output_add_inventory.csv')
    o_fulfilled = additional_o_inventory['qty_used'].sum().astype(int)
    o_fulfilled = o_fulfilled + fulfilled_start

    results_list_o = []
    results_list_o.append({
        'model': '+ Optimization'
        , 'error_pct': 0.0
        , 'waste_pcs': o_w_total
        , 'shortage_pcs': shortages_start
        , 'loss_pct': round((o_w_total + shortages_start) / total_demand * 100, 2)
        , 'fulfilled_d': o_fulfilled
        , 'fulfilled_pct': round(o_fulfilled / total_demand * 100, 2)
    })
    results_df_o = pd.DataFrame(results_list_o)

    return results_df_o