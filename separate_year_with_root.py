
import pandas as pd
def build_separate_year_with_root(df):
    def create_node(row, level, parent_key):
        return {
            "id": str(row.name + 1),
            "name": row[level],
            "value": row["revenue_diff"],
            "levelDescription": level,
            "levelNo": levels.index(level),
            "levelKey": f"{parent_key}-{row[level]}" if parent_key else str(row[level]),
            "children": []
        }

    levels = ["year", "division", "department_description", "class_description", "subclass_description"]
    combined_structure = {"root": []}

    # Group by years to create separate root entries for each year
    for year, group in df.groupby("year"):
        year_structure = {
            "root": {
                "id": str(group.index[0] + 1),
                "name": str(year),
                "value": group["revenue_diff"].sum(),  # Sum value for the year
                "levelDescription": "year",
                "levelNo": 0,
                "levelKey": str(year),
                "children": []
            }
        }
        
        for _, row in group.iterrows():
            current_node = year_structure["root"]
            parent_key = str(year)
            for level in levels[1:]:  # Start from 'division' as root is 'year'
                if pd.notna(row[level]):
                    parent_key = current_node["levelKey"]
                    children = current_node["children"]
                    # Check if the child node already exists
                    child_node = next((child for child in children if child["name"] == row[level]), None)
                    if not child_node:
                        child_node = create_node(row, level, parent_key)
                        children.append(child_node)
                    current_node = child_node
            
            # Update the value at the leaf node
            current_node["value"] = row["revenue_diff"]
        
        combined_structure["root"].append(year_structure)

    return combined_structure