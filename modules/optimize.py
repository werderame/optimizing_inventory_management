# optimize.py

import os
import pandas as pd
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, LpInteger, GLPK_CMD
import psycopg2
from . import db_config as db

# === DIR Config ===
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.join(base_dir, "data", "db_ready")
model_dir = os.path.join(base_dir, "data", "model_output")


# === DB Configuration ===
DB_NAME=db.DB_NAME
DB_USER=db.DB_USER
DB_PASSWORD=db.DB_PASSWORD
DB_HOST=db.DB_HOST
DB_PORT=db.DB_PORT


def optimize_inventory(gap=0.05): # gap here is used to lower the accuracy and speed up the solution

    # === 1.DATASET SETUP ===

    """ 1.1 Load relevant data """
    articles = pd.read_csv(os.path.join(db_dir, 'article_table.csv'))
    ingredients = pd.read_csv(os.path.join(db_dir, 'ingredient_table.csv'))
    recipes = pd.read_csv(os.path.join(db_dir, 'recipe_table.csv'))
    demand = pd.read_csv(os.path.join(db_dir, 'demand_table.csv'))
    i = pd.read_csv(os.path.join(model_dir, 'i.csv')) # inventory development (for availability)

    """ 1.2 Process demand """
    demand['demand_date'] = pd.to_datetime(demand['demand_date'])
    days = sorted(demand['demand_date'].dt.strftime('%Y-%m-%d').unique().tolist())

    """ 1.3 Create BOM: recipe_id, art_code, util_coeff """
    bom = (
        ingredients
        .merge(articles[['art_id', 'art_code']], on='art_id', how='left')
        [['recipe_id', 'art_code', 'util_coeff']]
    )
    recipes = bom['recipe_id'].unique()
    recipes = [int(r) for r in bom['recipe_id'].unique()]  # force int

    """ 1.4 Prep Inventory Data from the remaining inventory after Perfect FEFO allocation """
    i = i.sort_values(['art_code', 'inv_id'])
    available_inv = i[i['end_inventory_q'] > 0]
    available_inv = (
        available_inv.groupby(['inv_id', 'art_code', 'expiration_date'], as_index=False)
        .agg({'end_inventory_q': 'last'})  # last known remaining quantity
    )
    available_inv = available_inv.rename(columns={'end_inventory_q': 'remaining_q'})
    available_inv['expiration_date'] = pd.to_datetime(available_inv['expiration_date'])
    available_inv = available_inv[['art_code', 'expiration_date', 'remaining_q', 'inv_id']]

    """ 1.5 Caclulate the demand per day """
    rdem = (demand    
        .groupby('demand_date', as_index=False)
        .agg(total_recipes=('demand_q', 'sum'))
    )
    rdem['demand_date'] = rdem['demand_date'].dt.strftime('%Y-%m-%d')  # convert to string
    rdem['demand_date'] = rdem['demand_date'].astype(str)
    prod_cap = 1200

    
    #################################################
    """ 1.6 Use the following box to limit the dataset for testing purposes """

    # === SHRINK SETS FOR TESTS ===
    # ⬇️ Limit to first recipes
    sample_recipes = bom['recipe_id'].unique()[:] # Adjust to limit the number of recipes
    bom = bom[bom['recipe_id'].isin(sample_recipes)]

    # ⬇️ Limit to first days
    days = days[:] # Adjust to limit the number of days

    # ⬇️ Keep only inventory relevant to selected BOM art_codes
    relevant_art_codes = bom['art_code'].unique()
    available_inv = available_inv[available_inv['art_code'].isin(relevant_art_codes)]

    #################################################


    # === 2. Decision Variables ===
    
    """ 2.1 How many of each recipe to make each day"""
    # Create a dict of recipe-day (r, d) pairs and calculate the number of r for each d (x_r_d)
    x = {(r, d): LpVariable(f"x_{r}_{d}", lowBound=0, cat=LpInteger) # where r is recipe and d is day
        for r in recipes for d in days}

    """ 2.2 Decide what inventory batch to use for each day """
    # Auxiliary: inventory usage by inv_id per day
    y = {(iid, d): LpVariable(f"y_{iid}_{d}", lowBound=0)
        for iid in available_inv['inv_id'].unique() for d in days}

    # === 3. Objective Function ===

    """ 3.1 Maximize the total number of recipes made """
    model = LpProblem("Inventory_Usage_Optimization", LpMaximize)
    model += lpSum(x[r, d] for r in recipes for d in days)

    # === 4. Constraints ===

    """ 4.1 Ingredient demand must be met by available pallets"""
    for d in days:
        day_dt = pd.to_datetime(d)
        for art in bom['art_code'].unique():
            total_required = []
            for r in recipes:
                coeff_row = bom[(bom['recipe_id'] == r) & (bom['art_code'] == art)] # for each article of each recipe
                if not coeff_row.empty:
                    coeff = coeff_row.iloc[0]['util_coeff']
                    total_required.append(x[r, d] * coeff) # no. of articles needed for this recipe on this day

            inv_ids = available_inv[
                (available_inv['art_code'] == art) &
                (available_inv['expiration_date'] >= day_dt) # make sure there is inventory & it's still valid
            ]['inv_id'].tolist()

            if total_required and inv_ids:
                model += lpSum(total_required) <= lpSum(y[iid, d] for iid in inv_ids), f"ingredient_usage_{art}_{d}"

    """ 4.2 Total usage of each pallet ≤ remaining quantity before expiration"""
    for iid in available_inv['inv_id'].unique():
        row = available_inv[available_inv['inv_id'] == iid].iloc[0]
        exp = row['expiration_date']
        max_q = row['remaining_q']
        valid_days = [d for d in days if pd.to_datetime(d) <= exp]
        if valid_days:
            model += lpSum(y[iid, d] for d in valid_days) <= max_q, f"pallet_limit_{iid}"

    """ 4.3 Daily production must not exceed (capacity - forecasted demand)"""
    for d in days:
        demand_q = rdem.loc[rdem['demand_date'] == d, 'total_recipes'].values
        if demand_q.size > 0:
            free_capacity = prod_cap - demand_q[0]
            model += lpSum(x[r, d] for r in recipes) <= free_capacity, f"daily_capacity_{d}"
        else:
            raise ValueError(f"No demand data found for day {d}")
        
        
    # === 5. Solve ===

    solver = GLPK_CMD(
                    path="/opt/homebrew/bin/glpsol", 
                    msg=True,
                    options=["--mipgap", str(gap)]
                    )
    model.solve(solver)

    # === 6. Output ===

    """ 6.1 Recipes"""
    solution = pd.DataFrame([
        {
            "recipe_id": r, 
            "demand_date": d, 
            "demand_q": int(x[r, d].value())
        }
        for r in recipes for d in days
        if x[r, d].value() is not None and x[r, d].value() > 0
    ])

    """ 6.2 Inventory """
    inv_usage = [
        {
            "day": d,
            "inv_id": iid,
            "art_code": available_inv.loc[available_inv['inv_id'] == iid, 'art_code'].values[0],
            "qty_used": y[iid, d].value()
        }
        for (iid, d) in y
        if y[iid, d].value() is not None and y[iid, d].value() > 0
    ]
    inv_usage_df = pd.DataFrame(inv_usage)

    # === 7. Save to CSV ===
    solution = solution[['recipe_id', 'demand_q', 'demand_date']].sort_values(['demand_date', 'recipe_id'])
    solution.to_csv(os.path.join(model_dir, 'solver_solution.csv'), index=False)
    inv_usage_df.to_csv(os.path.join(model_dir, 'output_add_inventory.csv'), index=False)
    print('Solution found! Solver Solution and Inventory Usage CSVs available in the folder model_output.')
    return solution

# === 8. Push to Database ===
def publish_solution(demand_suggested):

    """ 8.1 Get unique IDs """
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        cur.execute(f"SELECT demand_id FROM demand order by demand_id desc limit 1;")
        demand_id = cur.fetchone() # get the last demand_id so to use unique ones
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error fetching the demand ID: {e}")

    """ 8.2 Assign ID to new rows """
    next_id = demand_id[0] + 1
    demand_suggested['demand_id'] = 0
    for i, row in demand_suggested.iterrows():
        demand_suggested.loc[i, 'demand_id'] = next_id
        next_id += 1

    demand_suggested = demand_suggested[['demand_id','recipe_id','demand_q','demand_date']]

    """ 8.3 Publish Optimal Menu Production solution in a separate demand_suggested table in DB """
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cur = conn.cursor()
        import io

        buffer = io.StringIO()
        demand_suggested.to_csv(buffer, index=False, header=False) # Convert df to CSV buffer (no index, no header)
        buffer.seek(0)

        # Execute
        cur.copy_from(buffer, 'demand_suggested', sep=',', columns=demand_suggested.columns) # populate a separate demand table
        conn.commit()
        cur.close()
        conn.close()
        print("🎯 Optimised menu production completed and available in the DB.")

    except Exception as e:
        print(f"❌ Error loading data in inventory: {e}")

""" end of script"""