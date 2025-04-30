import os
import json
import pandas as pd
import numpy as np
from parse_xls import parse_xls
from config import Config

class ARCParser:

    def __init__(self):
        self.file_path = Config.ARC_LIST_PATH
        self.arc_codes = None
        self.arc_hierarchy_dict = {}

    
    def generate_arc_dataframe(self):
        """
        Generate a DataFrame from the ARC List Excel file.
        """
        try:
            # Parse the Excel file
            arc_data = parse_xls(self.file_path)
            if arc_data is None:
                raise ValueError("Failed to parse the Excel file.")

            # Extract the relevant sheet
            arc_df = pd.DataFrame(arc_data['Sheet1'])
            
            # Ensure the DataFrame has proper column headers
            # This will rename existing columns or create them if they don't exist
            if arc_df.columns.tolist() != ['ARC', 'Description']:
                # If the DataFrame doesn't have headers, assign them
                # Assuming the first column is ARC and second is Description
                if len(arc_df.columns) >= 2:
                    arc_df.columns = ['ARC', 'Description'] + list(arc_df.columns[2:])
                else:
                    # If there aren't enough columns, create them
                    if len(arc_df.columns) == 1:
                        arc_df['Description'] = None
                    elif len(arc_df.columns) == 0:
                        arc_df = pd.DataFrame(columns=['ARC', 'Description'])

            print(arc_df.head())  # Debugging line to check the DataFrame content

            # MAP "ARC" and "Description" to a dictionary for self.arc_codes
            self.arc_codes = arc_df.set_index('ARC')['Description'].to_dict()
            
            return arc_df

        except Exception as e:
            print(f"Error generating ARC DataFrame: {e}")
            return None
            
    def generate_arc_hierarchy_tree(self):
        """
        Generate a hierarchical tree structure from a flat dictionary of ARC codes,
        where each digit represents a level in the hierarchy.
        
        For example:
        - 4.811 is a child of 4.81, which is a child of 4.8, which is a child of 4
        
        Returns:
            dict: A nested dictionary representing the hierarchical structure of the ARC codes.
        """
        # Initialize the tree
        hierarchy_tree = {}
        
        # Process each ARC code
        for arc_code, description in self.arc_codes.items():
            # Convert arc_code to string to handle float values
            arc_code_str = str(arc_code)
            
            # Remove any trailing zeros after decimal point for cleaner representation
            if '.' in arc_code_str:
                arc_code_str = arc_code_str.rstrip('0').rstrip('.')
            
            # Create a list of all parent codes
            parent_codes = []
            current_code = ""
            
            # Handle the digits before the decimal point
            before_decimal = arc_code_str.split('.')[0]
            for i, digit in enumerate(before_decimal):
                current_code += digit
                parent_codes.append(current_code)
            
            # Handle the digits after the decimal point, if any
            if '.' in arc_code_str:
                current_code += '.'
                after_decimal = arc_code_str.split('.')[1]
                for digit in after_decimal:
                    current_code += digit
                    parent_codes.append(current_code)
            
            # Build hierarchy starting from the top
            current = hierarchy_tree
            for i, code in enumerate(parent_codes):
                # If this is the final code (the original arc_code)
                if i == len(parent_codes) - 1:
                    current[code] = {
                        "code": code,
                        "description": description,
                        "children": {}
                    }
                else:
                    if code not in current:
                        current[code] = {
                            "code": code,
                            "description": self.arc_codes.get(code) or self.arc_codes.get(float(code)) if code.replace('.', '').isdigit() else None,
                            "children": {}
                        }
                    current = current[code]["children"]
        
        self.arc_hierarchy_dict = hierarchy_tree
        return hierarchy_tree
    
    def upload_arc_data(self, output_path=Config.ARC_JSON_PATH):
        """
        Upload the hierarchical tree structure to a database or other storage.
        """
        # Placeholder for upload logic
        # This could involve database connections, API calls, etc.
        
        parser_data = {
            "arc_hierarchy": self.arc_hierarchy_dict,
            "arc_codes": self.arc_codes
        }

        # Example: Save to a JSON file
        with open(output_path, 'w') as f:
            json.dump(parser_data, f, indent=4)

    

if __name__ == "__main__":
    parser = ARCParser()
    arc_df = parser.generate_arc_dataframe()
    
    print(parser.arc_codes)  # Debugging line to check the arc_codes dictionary
    parser.generate_arc_hierarchy_tree()
    print(parser.arc_hierarchy_dict)  # Debugging line to check the hierarchy tree
    parser.upload_arc_data()