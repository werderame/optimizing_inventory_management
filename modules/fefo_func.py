# fefo model

import os
import pandas as pd
import random
import time

rng = random.Random(42)
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = os.path.join(base_dir, "data", "model_output")


def fefo_daily (demand, inventory, num=0, output_names=None, rng=None):
    rng = rng or random

    """introduces the randomness in the inventory and allocates daily"""

    # Convert to list of dictionaries
    inventory = inventory.to_dict(orient="records")
    demand = demand.to_dict(orient="records")

    # Step 1: Convert dates and Sort Data
    for row in demand:
        row['demand_date'] = pd.to_datetime(row['demand_date'])
        row['end_demand_q'] = row['art_demand']

    for row in inventory:
        row['expiration_date'] = pd.to_datetime(row['expiration_date'])

    # Sort demand & inventory by date
    demand = sorted(demand, key=lambda x: x['demand_date'])
    inventory = sorted(inventory, key=lambda x: x['expiration_date'])

    # move a percentage of the rows to a separate list and then re-insert it randomly in the inventory
    num_move = max(0, int(len(inventory) * num)) # num% or at least [choose] 
    to_move = random.sample(inventory, num_move) 
    inventory = [row for row in inventory if row not in to_move] # reposition the ordered inventory 

    for i in range(len(to_move)):
        inventory.insert(random.randint(0,len(inventory)), to_move[i])

    # Organize inventory by article
    inventory_dict = {}
    for element in inventory:
        art = element['art_code']
        element['end_inventory_q'] = element['quantity']
        element['waste_inventory_q'] = 0
        if art not in inventory_dict:
            inventory_dict[art] = []
        inventory_dict[art].append(element)

    # Output Containers
    output_demand = []
    output_inventory = []
    output_waste = []

    # Step 2: Process Demand
    for row in demand:
        demand_article = row['art_code']
        demand_qty = row['art_demand']
        demand_date = row['demand_date']
        remaining_demand_qty = row['end_demand_q']
        
        demand_fulfilled = 0

        # Step 2.1: Remove expired inventory
        if demand_article in inventory_dict:
            expired_batches = []
            for irow in inventory_dict[demand_article]:
                if irow['expiration_date'] <= demand_date and irow['end_inventory_q'] > 0:
                    irow['waste_inventory_q'] = irow['end_inventory_q']
                    expired_batches.append({
                        "art_code": irow['art_code'],
                        "inv_id": irow['inv_id'],
                        "nominal_inventory_q": irow['quantity'],
                        "expired_quantity": irow['waste_inventory_q'],
                        "expiration_date": irow['expiration_date']
                    })
                    irow['end_inventory_q'] = 0  # Set remaining inventory to 0

            # Append all expired inventory at this date
            output_waste.extend(expired_batches)

        # Step 2.2: Fulfill Demand
        if demand_article in inventory_dict:
            for irow in inventory_dict[demand_article]:
                # SKIP expired batches (already recorded as waste)
                if irow['end_inventory_q'] == 0:
                    continue  # This ensures expired batches aren't processed again
                if remaining_demand_qty > 0 and irow['end_inventory_q'] > 0:
                    use_qty = min(remaining_demand_qty, irow['end_inventory_q'])
                    demand_fulfilled += use_qty
                    start_inventory_q = irow['end_inventory_q']
                    irow['end_inventory_q'] -= use_qty
                    remaining_demand_qty -= use_qty
            
                    # Store demand fulfillment
                    output_demand.append({
                        'art_code': demand_article,
                        'demand_date': demand_date,
                        'nominal_demand_q': row['art_demand'],
                        'fulfilled_demand_q': use_qty,
                        'remaining_demand_q': remaining_demand_qty,
                        'processing_date': demand_date, # When this demand was processed
                        "expiration_date": irow['expiration_date']
                    })


                    # Pass the rest onto the output inventory
                    output_inventory.append({
                        "art_code": irow["art_code"],
                        "inv_id": irow['inv_id'],
                        "nominal_inventory_q": irow['quantity'],
                        "start_inventory_q": start_inventory_q,
                        "used_inventory": use_qty,
                        "end_inventory_q": irow["end_inventory_q"],
                        "expiration_date": irow["expiration_date"],
                        "last_processed_date": demand_date  # Last date this inventory was checked
                    })

        # Step 2.3: Handle Unmet Demand
        if remaining_demand_qty > 0:
            output_demand.append({
                'art_code': demand_article,
                'demand_date': demand_date,
                'nominal_demand_q': row['art_demand'],
                'fulfilled_demand_q': 0,
                'remaining_demand_q': remaining_demand_qty,
                'processing_date': demand_date,  # Still attempted to process on this date
                "expiration_date": irow['expiration_date']
            })

    # Step 3: Process Remaining Inventory
    for art_key, inventory_list in inventory_dict.items():
        for irow in inventory_list:
            expired_batches = []
            # SKIP inventory that is already expired and set to zero
            if irow['end_inventory_q'] == 0:
                continue 
            if irow['end_inventory_q'] > 0 and irow['expiration_date'] <= demand_date:
                irow['waste_inventory_q'] = irow['end_inventory_q']
                expired_batches.append({
                    "art_code": irow['art_code'],
                    "inv_id": irow['inv_id'],
                    "nominal_inventory_q": irow['quantity'],
                    "expired_quantity": irow['waste_inventory_q'],
                    "expiration_date": irow['expiration_date']
                })
                irow['end_inventory_q'] = 0  # Set remaining inventory to 0

        # Append all expired inventory at this date
        output_waste.extend(expired_batches)

        # Pass the rest onto the output inventory
        if irow['end_inventory_q'] > 0:
            output_inventory.append({
                "art_code": irow["art_code"],
                "inv_id": irow['inv_id'],
                "nominal_inventory_q": irow['quantity'],
                "start_inventory_q": start_inventory_q,
                "end_inventory_q": irow["end_inventory_q"],
                "expiration_date": irow["expiration_date"],
                "last_processed_date": demand_date  # Last date this inventory was checked
            })

    # Display Results
    output_demand = pd.DataFrame(output_demand)
    output_inventory = pd.DataFrame(output_inventory)
    output_waste = pd.DataFrame(output_waste)

    # Dump Sets
    outputs = [output_demand, output_inventory, output_waste]
    if output_names:
        for name, df in zip(output_names, outputs):
            df.to_csv(os.path.join(output_dir, f"{name}.csv"), index=False)

    return output_demand, output_inventory, output_waste