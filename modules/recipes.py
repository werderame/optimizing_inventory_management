# recipes.py

import os
import json
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from thefuzz import fuzz, process

# === 0. Set Directories ===
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
raw_dir = os.path.join(base_dir, "data", "raw")
processed_dir = os.path.join(base_dir, "data", "processed")

def generate_hellofresh_bom():

    headers = {"User-Agent": "Mozilla/5.0"}

    # === 1. Load or Initialize Datasets ===
    """ 1.1 Load recipe URLs from HelloFresh JSON file""" 
    # RECIPE LINKS
    recipe_url_path = os.path.join(raw_dir, "hellofresh_recipe_urls.json")
    with open(recipe_url_path, "r", encoding="utf-8") as file:
        recipe_links = list(set(json.load(file)))
    print(f"📁 Loaded {len(recipe_links)} recipe URLs from file.")

    # slice the list to shorten scraping time
    recipe_links = recipe_links[:7]  # Adjust the number as needed for testing

    """ 1.2 Load Inventory Article Master Data""" 
    # SKUS
    skus = pd.read_csv(os.path.join(processed_dir, "my_inventory.csv"))

    """ 1.3 Initialize Article Descriptions DataFrame"""
    # INGREDIENTS
    ingredients = pd.DataFrame(columns=["ingredient_name", "quantity", "full_description"])

    """ 1.4 Initialize BOM DataFrame """
    # BOM ----> preliminary Bill of Materials containing information scraped from HelloFresh recipes
    bom_df = pd.DataFrame(columns=["recipe_name", "url", "ingredient_name", "quantity", "tags", "full_description"])

    """ 1.5 Initialize CLEAN BOM DataFrame """
    # CLEAN BOM ----> enriched Bill of Materials matching information from our Inventory
    clean_bom = pd.DataFrame() # leave empty to adjust columns later


    # === 2. Scrape each recipe URL for ingredients and tags ===
    all_recipes = []
    for counter, recipe_url in enumerate(recipe_links, 1):
        try:
            response = requests.get(recipe_url, headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")

            title_tag = soup.find("h1")
            title = title_tag.text.strip() if title_tag else "Unknown Recipe"
            print(f"🔍 Scraping {counter}/{len(recipe_links)}: {title}")

            ing_list = []
            for div in soup.find_all("div", {"data-test-id": "ingredient-item-shipped"}):
                quantity = div.find("p", class_="hvELYs")
                name = div.find("p", class_="fQwNKo")
                ing_list.append({
                    "quantity": quantity.text.strip() if quantity else "Unknown Quantity",
                    "ingredient": name.text.strip() if name else "Unknown Ingredient"
                })

            tags = [tag.get_text(strip=True).replace("•", "") for tag in soup.find_all("div", {"data-test-id": "recipe-description-tag"})]

            all_recipes.append({
                "title": title,
                "url": recipe_url,
                "ingredients": ing_list,
                "tags": tags
            })

            time.sleep(1)

        except Exception as e:
            print(f"❌ Error scraping {recipe_url}: {e}")

    # === 3. Flatten into BOM DataFrame ===
    bom_data = []
    for recipe in all_recipes:
        for ing in recipe["ingredients"]:
            bom_data.append({
                "recipe_name": recipe["title"],
                "url": recipe["url"],
                "ingredient_name": ing["ingredient"],
                "quantity": ing["quantity"],
                "tags": recipe["tags"]
            })
    bom_df = pd.DataFrame(bom_data)
    bom_df['full_description'] = (bom_df['ingredient_name'] + ' - ' + bom_df['quantity']).str.lower()

    # === 4. Save outputs ===
    """4.1 Recipes """
    # BOM
    os.makedirs(processed_dir, exist_ok=True)
    #bom_df.to_csv(os.path.join(processed_dir, "recipes_bom.csv"), index=False) # do not save / do not alter the stored csv - we skip the scraping and use the already dumped csv.

    """4.2 Article Descriptions """
    # INGREDIENTS
    ingredients = bom_df[['ingredient_name', 'quantity', 'full_description']].drop_duplicates().sort_values(by='full_description')

    print(f"Recipes Descriptions: ({len(ingredients)} lines)")
    print(f"BOM df: ({len(bom_df)} lines)")

    # === 5. Match Ingredients (HelloFresh) to SKUs (Inventory)
    """ 5.1 Clean and deduplicate SKUs """
    skus = skus[['art_code', 'art_name', 'art_category']].drop_duplicates()
    skus['art_name'] = skus['art_name'].str.replace(" - (v)", "", regex=False)
    skus['art_name'] = skus['art_name'].str.replace(" - qs", "", regex=False)
    skus['art_name'] = skus['art_name'].str.replace(" - rth", "", regex=False)
    skus['art_name'] = skus['art_name'].str.replace(r"([0-9]{1,3})[g]", r"\1 g", regex=True)
    skus['art_name'] = skus['art_name'].str.replace(r"([0-9]{1,3})[ml]", r"\1 ml", regex=True)

    """ 5.2 Prepare lists for matching """
    inv_desc = skus['art_name'].tolist()
    ing_desc = ingredients['full_description'].tolist()

    """ 5.3 Run matching """
    matches_list = []
    for ingredient in ing_desc:
        best_match = process.extractOne(ingredient, inv_desc, scorer=fuzz.token_set_ratio)
        matches_list.append({
            'ing': ingredient,
            'sku': best_match[0],
            'scr': best_match[1]
        })

    matches_df = pd.DataFrame(matches_list).sort_values('scr')

    """ 5.4 Merge with SKU Data """
    skus['count'] = 1
    ingredients_skus = pd.merge(matches_df, skus, how='left', left_on='sku', right_on='art_name')
    ingredients_skus = ingredients_skus[['ing', 'art_code', 'count']]

    """ 5.5 Save Output"""
    # This allows to manually check the matches and reconcile if necessary
    ingredients_skus.to_csv(os.path.join(processed_dir, "ingredient_skus.csv"), index=False)

    # MANUAL BOM ----> intermediate manual step to reconcile the matches and the coefficients

    # === 6. BOM building ===
    """ 6.1 Merge HelloFresh BOM with reconciled SKUs """
    manual_bom = pd.read_csv(os.path.join(processed_dir, "ingredient_skus_reconciled.csv"))
    bom_df = pd.read_csv(os.path.join(processed_dir, "recipes_bom.csv")) # this df is already loaded via this very script, however I want to keep the loading to be able to skip the scraping step in the future
    bom_match = pd.merge(bom_df, manual_bom, how='left', left_on='full_description', right_on='ing')

    """ 6.2 Check the accuracy of the matches"""
    missing_matches = bom_match['full_description'][bom_match['art_code'].isna()].to_frame()
    missing_lines = len(missing_matches)
    count_missing = len(missing_matches.drop_duplicates())
    unique_art_codes = bom_match['art_code'].nunique()
    print(f"Lines with an unknown art code: {missing_lines}, made of {count_missing} unique articles ({round(missing_lines/len(bom_match) * 100,1)}% of the entire dataset) -> these are goind to be sacrificed.\n")

    # === 7. Clean and Save the BOM ===
    """ 7.1 Clean """
    clean_bom = bom_match.drop(columns=['ing'])
    clean_bom = clean_bom[clean_bom['art_code'].notna()]
    clean_bom['count'] = clean_bom['count'].astype('Int64')
    clean_bom = clean_bom.rename(columns={'quantity': 'ingredient_qty', 'count':'util_coeff'})
    clean_bom = clean_bom.drop_duplicates()

    """ 7.2 Store the Clean BOM """
    clean_bom.to_csv(os.path.join(processed_dir, "clean_bom.csv"), index=False)
    print("BOM stored as CSV")
    pass

if __name__ == "__main__":
    generate_hellofresh_bom()