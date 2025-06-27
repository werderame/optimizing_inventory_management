# Menu Planning Script

from datetime import datetime as dt, timedelta as td
import random
import pandas as pd
import ast
import numpy as np
import os

# === DIR Config ===
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_dir = os.path.join(base_dir, "data", "db_ready")
processed_dir = os.path.join(base_dir, "data", "processed")

def plan_menu(min_meals=950, max_meals=1050, weeks=6, start_date='2025-05-01'):

    random.seed(42) # reproduce results
    np.random.seed(42)

    # === 1. Load or Initialize Datasets ===

    """ 1.1 Load the BOM to work with the tags"""
    clean_bom = pd.read_csv(os.path.join(processed_dir, "clean_bom.csv"))
    tagged_recipes = clean_bom[['recipe_name', 'tags']]
    tagged_recipes = tagged_recipes.drop_duplicates().reset_index(drop=True)
    tagged_recipes['recipe_id'] = tagged_recipes.index

    """ 1.2 Load inventory Article Master Data"""
    skus = pd.read_csv(os.path.join(processed_dir, "my_inventory.csv"))

    """ 1.1 Initialize the DB table Recipe"""
    recipe_df = pd.DataFrame(columns=['recipe_id', 'recipe_name'])

    """ 1.x Initialize the DB table Demand"""
    demand_df = pd.DataFrame(columns=['demand_id', 'recipe_id', 'demand_q', 'demand_date'])

    """ 1.x Initialize the DB table Article"""
    article_df = pd.DataFrame()

    """ 1.x Initialize the DB table Ingredient"""
    ingredient_df = pd.DataFrame()

    # === 2. Use TAGS to generate a realistic demand ===

    fav_recipes = tagged_recipes[['recipe_id', 'tags']]
    fav_recipes['tags'] = fav_recipes['tags'].apply(ast.literal_eval) # evaluate the tag strings as lists
    fav_recipes = fav_recipes.explode('tags')
    fav_recipes['fav_score'] = 1 # initialize the score with 1
    favourites = ['Family', 'Schnell', 'High Protein', 'Vegetarisch', 'Fit & Fun'] # Create a list of favourite Tags

    """ assign a favourite score to the recipes to draw those more probably"""
    for index, tag in fav_recipes['tags'].items():
        fav_score = 1
        if tag in favourites:
            fav_score += 2
        fav_recipes.loc[index, 'fav_score'] = fav_score
    fav_recipes = fav_recipes.groupby('recipe_id', as_index=False)['fav_score'].sum()
    fav_recipes.head()

    # === 3. Calculate Total Recipe Sales ===

    sd = dt.strptime(start_date,'%Y-%m-%d')

    """ Generate a list of dates and total demand for each day using a random generator and min and max limits. """
    overall_demand = []
    for i in range(weeks * 7): # multiply by 7 days per week
        overall_demand.append({
            'day': sd,
            'tot_dem': random.randint(min_meals, max_meals) # assign a total demand daily that varies between these two values
        })
        sd += td(days=1)


    # === 4. Distribute Sales to Daily Recipe demand ===

    """Create the Demand table using a random generator with a probability distribution based on favourite recipes.
    Loop through the overall demand and allocate half of the remaining demand to each recipe."""
    recipe_demand = []
    for row in overall_demand: 
        remaining_dem = row['tot_dem']
        while int(remaining_dem) > 0:
            x = random.randint(1,int(remaining_dem/2)+1) # allocate half of the remaining demand to a recipe
            remaining_dem -= x # reduce the remaining demand
            draw = np.random.choice(fav_recipes['recipe_id'],  p=fav_recipes['fav_score'] / fav_recipes['fav_score'].sum()) # choose a recipe based on the score probability distribution
            recipe_demand.append({
                'demand_date': row['day'],
                'recipe_id': draw,
                'demand_q': x
            })
    len(recipe_demand)
    demand_df = pd.DataFrame(recipe_demand)


    # === 5. Return a stat on demand of recipes ===

    agg = demand_df.groupby('recipe_id')['demand_q'].sum().sort_values(ascending=False)
    total_demand = agg.sum()
    cumulative = agg.cumsum() / total_demand
    top_80_count = (cumulative <= 0.80).sum() # Recipes that together make up the top 80% of demand
    bottom_20_count = (cumulative >= 0.80).sum() # Recipes that make up the bottom 20% of demand
    print(f"Recipes making up top 80% of total demand: {top_80_count}")
    print(f"Recipes making up bottom 20% of total demand: {bottom_20_count}")

    # === 6. Push the ouptupt to a DB READY directory ===

    """ 6.1 Dump the demand table to the db datasest """
    demand_df['demand_id'] = demand_df.index
    demand_df = demand_df[['demand_id', 'recipe_id', 'demand_q', 'demand_date']]
    demand_df.to_csv(os.path.join(db_dir, "demand_table.csv"), index=False)

    """ 6.2 Dump the recipe table to the db datasets"""
    recipe_df = tagged_recipes[['recipe_id', 'recipe_name']].drop_duplicates().reset_index(drop=True)
    recipe_df.to_csv(os.path.join(db_dir, "recipe_table.csv"), index=False)

    """ 6.3 Dump the article table to the db datasets"""
    art_unique = clean_bom['art_code'].drop_duplicates().reset_index(drop=True)
    hf_inventory = skus[['art_code', 'art_name', 'art_category', 'shelf_life']].drop_duplicates()
    article_df = pd.merge(art_unique, hf_inventory, how='left', on='art_code')
    article_df['art_id'] = article_df.index
    article_df = article_df[['art_id', 'art_code', 'art_name', 'art_category', 'shelf_life']]
    article_df.to_csv(os.path.join(db_dir, "article_table.csv"), index=False)

    """ 6.4 Dump the ingredient table to the db datasets"""
    ingredient_df = pd.merge(clean_bom, recipe_df, on='recipe_name')
    art_ids = article_df[['art_id', 'art_code']]
    ingredient_df = pd.merge(ingredient_df, art_ids, on='art_code')
    ingredient_df = ingredient_df.drop(columns=['art_code', 'recipe_name'])
    ingredient_df['ingredient_id'] = ingredient_df.index
    ingredient_df = ingredient_df[['ingredient_id', 'recipe_id', 'art_id', 'util_coeff']]
    ingredient_df.to_csv(os.path.join(db_dir, "ingredient_table.csv"), index=False)

    print ("4 Tables successfully dumped to the DB READY directory:\n   - demand_table.csv,\n   - recipe_table.csv,\n   - article_table.csv,\n   - ingredient_table.csv")
    print("Next: Set up your DB and Purchase your ingredients to start cooking!")
    pass

if __name__ == "__main__":
    plan_menu()