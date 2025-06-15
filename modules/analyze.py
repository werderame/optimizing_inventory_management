from modules import fefo_func as ff
import os
import pandas as pd

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = os.path.join(base_dir, "data", "model_output")




def results(demand, inventory, errors):
    results_list = []
    for e, t in errors:
        id, ii, iw = ff.fefo_daily(demand, inventory, e)  
        
        # vars
        i_waste = iw['expired_quantity'].sum()
        i_fulfilled = id['fulfilled_demand_q'].sum()
        i_shortages = id.groupby(['art_code', 'demand_date'])['remaining_demand_q'].last().sum()
        total_demand = id[['art_code', 'demand_date', 'nominal_demand_q']].drop_duplicates()['nominal_demand_q'].sum()
        
        results_list.append({
        'model': f'{t}'
        , 'error_pct': e * 100
        , 'waste_pcs': i_waste
        , 'shortage_pcs': i_shortages
        , 'loss_pct': round((i_waste + i_shortages) / total_demand * 100, 2)
        , 'fulfilled_d': i_fulfilled
        , 'fulfilled_pct': round(i_fulfilled / total_demand * 100, 2)
    })
    results_df = pd.DataFrame(results_list)

    optimal_fefo_w = pd.read_csv(os.path.join(output_dir,'o_output_inventory.csv'))[['inv_id', 'qty_used']]
    optimal_fefo_w = (
        optimal_fefo_w
        .groupby(["inv_id"])
        .agg(
            total_qty_used=("qty_used", "sum")
        )
        .reset_index()) # Additional usage
    optimal_fefo_w['total_qty_used'] = optimal_fefo_w['total_qty_used'].astype(int)

    waste_pro = pd.merge(w, optimal_fefo_w, how='left').fillna(0)
    waste_pro = waste_pro.rename(columns={'total_qty_used': 'opt_fefo_usage'})
    waste_pro = waste_pro.rename(columns={'expired_quantity': 'per_fefo_w'})
    waste_pro['opt_fefo_w'] = waste_pro['per_fefo_w'] - waste_pro['opt_fefo_usage'].astype(int)
    # enforce only positive waste (negative would correspond to units in addition to customer demand)
    waste_pro['opt_fefo_w'] = waste_pro['opt_fefo_w'].clip(lower=0) 

    o_w_total = sum(waste_pro['opt_fefo_w'])

    shortage_at_zero = int(results_df.loc[results_df['error_pct'] == 0, 'shortage_pcs'].values[0])

    additional_o_inventory = pd.read_csv('./data/model_output/o_output_inventory.csv')
    o_fulfilled = additional_o_inventory['qty_used'].sum().astype(int)

    fulfilled_at_zero = int(results_df.loc[results_df['error_pct'] == 0, 'fulfilled_d'].values[0])


    results_list_o = []
    results_list_o.append({
        'model': '+ Optimization'
        , 'error_pct': 0.0
        , 'waste_pcs': o_w_total
        , 'shortage_pcs': shortage_at_zero
        , 'loss_pct': round(o_w_total + shortage_at_zero / total_demand * 100, 2)
        , 'fulfilled_d': fulfilled_at_zero + o_fulfilled
        , 'fulfilled_pct': round((fulfilled_at_zero + o_fulfilled) / total_demand * 100, 2)
    })
    results_df_o = pd.DataFrame(results_list_o)

    table = pd.concat([results_df, results_df_o], ignore_index=True)
    table.to_csv(os.path.join(output_dir, 'table.csv'), index=False)
    return table