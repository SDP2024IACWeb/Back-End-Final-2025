"""
Enhanced NAICS parser implementation with improved hierarchy building and range code handling.
"""



import os
import pandas as pd
import numpy as np
from parse_xls import parse_xls
from config import Config
class NAICSNode:
    """
    Represents a node in the NAICS classification tree.
    Each node corresponds to an industry code and its description.
    """
    def __init__(self, code, title):
        self.code = code                    # The NAICS code (as a string)
        self.title = title                  # The title/description of the industry
        self.children = {}                  # Dictionary of child nodes, keyed by their code
        self.parent = None                  # Reference to parent node
        self.is_range = '-' in code         # Flag for range codes (e.g., "44-45")
        self.alternate_codes = []           # Additional codes this node can be referenced by
        
    def add_child(self, child_node):
        """Add a child node to this node"""
        self.children[child_node.code] = child_node
        child_node.parent = self
    
    def add_alternate_code(self, alt_code):
        """Add an alternate code for this node"""
        if alt_code not in self.alternate_codes:
            self.alternate_codes.append(alt_code)
    
    def get_path(self):
        """Returns the path from root to this node"""
        if self.parent is None:
            return [self]
        return self.parent.get_path() + [self]
    
    def to_dict(self):
        """Convert node to dictionary format for API responses"""
        return {
            'code': self.code,
            'title': self.title,
            'is_range': self.is_range,
            'alternate_codes': self.alternate_codes,
            'has_children': len(self.children) > 0
        }
    
    def __str__(self):
        return f"{self.code}: {self.title}"
    
    def __repr__(self):
        return self.__str__()


class NAICSParser:
    """
    Enhanced parser for NAICS (North American Industry Classification System) codes.
    
    This class provides functionality to load, parse, and query
    NAICS codes from the Census Bureau's hierarchical classification system.
    Features include:
    
    1. Tree-based hierarchy representation for efficient traversal
    2. Proper handling of range codes (e.g., "44-45")
    3. Comprehensive search and query capabilities 
    4. Robust data loading with fallback mechanisms
    """
    
    def __init__(self):
        """Initialize the NAICS parser."""
        self.naics_data = None                  # DataFrame containing raw NAICS codes and titles
        self.root = NAICSNode("ROOT", "North American Industry Classification System")
        self.all_nodes = {}                     # Dictionary for direct code lookups
        self.code_aliases = {}                  # Maps alternate codes to their canonical codes
        
        # Load and parse the data
        self.load_naics_data()
        self.build_hierarchy()
    
    def load_naics_data(self):
        """
        Load NAICS data from the specified Excel file.
        
        Returns:
            pandas.DataFrame: DataFrame containing NAICS codes and descriptions.
        """
        naics_path = Config.NAICS_CODES_PATH
        naics_sheets = parse_xls(naics_path)
        
        if naics_sheets is None:
            raise FileNotFoundError(f"Could not find the NAICS file at {naics_path}")
        
        # Assuming the first sheet contains the NAICS codes
        sheet_name = list(naics_sheets.keys())[0]
        self.naics_data = naics_sheets[sheet_name]

        # Clean up data - handle different possible column names based on Census format
        # Try to identify the code and title columns
        columns = self.naics_data.columns

        # Try to find the "2022 NAICS US Code" and "2022 NAICS US Title" columns
        if '2022 NAICS US   Code' in columns and '2022 NAICS US   Title' in columns:
            code_col = '2022 NAICS US   Code'
            title_col = '2022 NAICS US   Title'
        else:
            # If not found, try to find columns with similar names
            code_col = next((col for col in columns if 'code' in str(col).lower()), None)
            title_col = next((col for col in columns if 'title' in str(col).lower() or 'description' in str(col).lower()), None)
            
            if not code_col or not title_col:
                raise ValueError("Could not find the required columns in the NAICS data.")
            
        # Select only the needed columns and drop rows with NaN codes
        self.naics_data = self.naics_data[[code_col, title_col]]
        self.naics_data = self.naics_data.dropna(subset=[code_col])
 
        # Log for debugging
        print(f"Loaded NAICS data with {len(self.naics_data)} entries")
        print(self.naics_data.head(5))  # Print the first 5 rows
        
        # Rename columns for consistency
        self.naics_data = self.naics_data.rename(columns={code_col: 'Code', title_col: 'Title'})

        # Ensure codes are strings and clean them
        self.naics_data['Code'] = self.naics_data['Code'].astype(str).str.strip()
        
        # Apply fixes to the NAICS data including range code handling
        self.naics_data = self.fix_naics_classifications(self.naics_data)
        
        return self.naics_data
    
    def fix_naics_classifications(self, naics_data):
        """
        Fix known classification issues in the NAICS data.
        
        This method handles range codes (e.g., "44-45") by creating additional entries
        for each individual code in the range while preserving the original range entry.
        
        Args:
            naics_data (pandas.DataFrame): DataFrame containing NAICS codes and titles
            
        Returns:
            pandas.DataFrame: Corrected NAICS data with expanded range codes
        """
        # Make a copy to avoid modifying the original
        corrected_data = naics_data.copy()

        # Identify rows with range codes (containing a dash)
        mask_range = corrected_data['Code'].str.contains('-')
        range_rows = corrected_data[mask_range].copy()
        
        # Only proceed if range codes are found
        if not range_rows.empty:
            print(f"Found {len(range_rows)} range codes to process")
            
            # Create a list for new rows
            new_rows = []
            
            # Process each range code
            for _, row in range_rows.iterrows():
                code_range = row['Code']
                title = row['Title']
                
                try:
                    # Split the range and create new rows for each code
                    code_parts = code_range.split('-')
                    if len(code_parts) == 2 and code_parts[0].isdigit() and code_parts[1].isdigit():
                        start_code = int(code_parts[0].strip())
                        end_code = int(code_parts[1].strip())
                        
                        # Range should be reasonable (to avoid errors)
                        if 0 < end_code - start_code < 100:
                            for code in range(start_code, end_code + 1):
                                new_row = row.copy()
                                new_row['Code'] = str(code)
                                new_row['IsPartOfRange'] = code_range  # Track origin
                                new_rows.append(new_row)
                            print(f"Expanded range code {code_range} into {end_code - start_code + 1} individual codes")
                except Exception as e:
                    print(f"Error processing range code {code_range}: {e}")
            
            # Add the new rows to the DataFrame
            if new_rows:
                # Ensure the original data has the IsPartOfRange column
                if 'IsPartOfRange' not in corrected_data.columns:
                    corrected_data['IsPartOfRange'] = None
                
                # Convert new rows to DataFrame and combine with original
                new_rows_df = pd.DataFrame(new_rows)
                
                # Keep the original range codes and add the expanded individual codes
                corrected_data = pd.concat([corrected_data, new_rows_df], ignore_index=True)
                
                print(f"Added {len(new_rows)} new rows from expanded range codes")
            
        return corrected_data
    
    def build_hierarchy(self):
        """
        Build a tree-based hierarchical structure of NAICS codes.
        
        This method creates a tree where:
        - Each node represents a NAICS code
        - Parent-child relationships follow the NAICS hierarchy
        - Range codes (e.g., "44-45") are properly handled
        - Aliases are created for individual codes in ranges
        
        Returns:
            dict: Stats about the built hierarchy (node count, etc.)
        """
        if self.naics_data is None:
            self.load_naics_data()
        
        # Reset current state if rebuilding
        self.all_nodes = {}
        self.code_aliases = {}
        
        # Sort data by code length to ensure parent nodes are processed first
        sorted_data = self.naics_data.copy()
        # Add a column for code length (without dashes)
        sorted_data['CodeLength'] = sorted_data['Code'].apply(lambda x: len(x.replace('-', '')))
        sorted_data = sorted_data.sort_values(by='CodeLength')
        
        # First pass: Add all nodes to the tree
        for _, row in sorted_data.iterrows():
            code = str(row['Code']).strip()
            title = row['Title']
            
            # Skip invalid codes
            if not code or code == 'nan':
                continue
            
            # Create the node
            node = NAICSNode(code, title)
            clean_code = code.replace(',', '').replace(' ', '')
            self.all_nodes[clean_code] = node
            
            # Handle range codes (e.g., "44-45")
            if '-' in clean_code and all(c.isdigit() or c == '-' for c in clean_code):
                try:
                    start_code, end_code = clean_code.split('-')
                    start = int(start_code)
                    end = int(end_code)
                    
                    # Register each individual code in the range as an alias
                    for code_num in range(start, end + 1):
                        alias_code = str(code_num)
                        if alias_code != clean_code:  # Don't alias to itself
                            self.code_aliases[alias_code] = clean_code
                            node.add_alternate_code(alias_code)
                except ValueError:
                    # If parsing fails, just continue without creating aliases
                    pass
        
        # Second pass: Establish parent-child relationships
        for code, node in self.all_nodes.items():
            # Skip range codes for special handling
            if '-' in code:
                # Top-level sectors or range codes go directly under root
                self.root.add_child(node)
                continue
                
            # Handle normal codes
            if len(code) == 2:
                # Top-level sectors connect directly to root
                self.root.add_child(node)
            else:
                # Find the parent by looking at progressively shorter prefixes
                parent_found = False
                for i in range(len(code)-1, 0, -1):
                    potential_parent_code = code[:i]
                    
                    # Check direct parent
                    if potential_parent_code in self.all_nodes:
                        self.all_nodes[potential_parent_code].add_child(node)
                        parent_found = True
                        break
                    
                    # Check if parent is aliased
                    if potential_parent_code in self.code_aliases:
                        canonical_code = self.code_aliases[potential_parent_code]
                        self.all_nodes[canonical_code].add_child(node)
                        parent_found = True
                        break
                
                # If no parent found, attach to root
                if not parent_found:
                    print(f"Warning: No parent found for {code}. Attaching to root.")
                    self.root.add_child(node)
        
        # Return stats about the built hierarchy
        return {
            'total_nodes': len(self.all_nodes),
            'total_aliases': len(self.code_aliases),
            'root_children': len(self.root.children)
        }
    
    def get_node(self, code):
        """
        Get a node by its code, handling aliases for range codes.
        
        Args:
            code: NAICS code to look up
            
        Returns:
            NAICSNode object or None if not found
        """
        clean_code = str(code).strip().replace(',', '').replace(' ', '')
        
        # Direct lookup
        if clean_code in self.all_nodes:
            return self.all_nodes[clean_code]
        
        # Check if this is an aliased code
        if clean_code in self.code_aliases:
            canonical_code = self.code_aliases[clean_code]
            return self.all_nodes.get(canonical_code)
        
        return None
    
    def get_code_info(self, code):
        """
        Get detailed information about a specific NAICS code.
        
        Args:
            code (str): NAICS code to query.
            
        Returns:
            dict: Information about the code including title, path, and children.
                  Returns None if code not found.
        """
        node = self.get_node(code)
        if not node:
            return None
        
        # Get the path to this node
        path = node.get_path()
        path_info = [n.to_dict() for n in path if n.code != "ROOT"]
        
        # Get children
        children = []
        for child_code, child_node in node.children.items():
            children.append(child_node.to_dict())
        
        # Sort children by code
        children.sort(key=lambda x: x['code'])
        
        return {
            'code': node.code,
            'title': node.title,
            'is_range': node.is_range,
            'alternate_codes': node.alternate_codes,
            'path': path_info,
            'children': children
        }
    
    def get_children(self, code):
        """
        Get all direct children of a specific NAICS code.
        
        Args:
            code (str): NAICS code to query.
            
        Returns:
            list: List of child nodes in dictionary format.
                  Returns empty list if code not found or has no children.
        """
        node = self.get_node(code)
        if not node:
            return []
        
        children = []
        for child_code, child_node in node.children.items():
            children.append(child_node.to_dict())
        
        # Sort children by code
        children.sort(key=lambda x: x['code'])
        return children
    
    def get_descendants(self, code, max_depth=None):
        """
        Get all descendants of a NAICS code, optionally limited by depth.
        
        Args:
            code (str): NAICS code to start from.
            max_depth (int, optional): Maximum depth to traverse.
            
        Returns:
            list: List of descendant nodes in dictionary format.
        """
        node = self.get_node(code)
        if not node:
            return []
        
        descendants = []
        self._collect_descendants(node, descendants, max_depth, 0)
        
        # Convert to dictionary format
        return [node.to_dict() for node in descendants]
    
    def _collect_descendants(self, node, result, max_depth, current_depth):
        """Helper method for get_descendants"""
        for child in node.children.values():
            result.append(child)
            if max_depth is None or current_depth < max_depth:
                self._collect_descendants(child, result, max_depth, current_depth + 1)
    
    def search_naics(self, query, max_results=100):
        """
        Enhanced search for NAICS codes by title or code.
        
        This method searches both code and title fields, with results
        prioritized based on relevance.
        
        Args:
            query (str): Search query string.
            max_results (int): Maximum number of results to return.
            
        Returns:
            list: List of matching NAICS codes in dictionary format.
        """
        if self.naics_data is None:
            self.load_naics_data()
        
        query = query.lower()
        results = []
        
        # Check for direct code match first
        node = self.get_node(query)
        if node:
            results.append(node.to_dict())
        
        # Check for code prefix match
        code_prefix_matches = []
        for code, node in self.all_nodes.items():
            if code.startswith(query):
                code_prefix_matches.append(node)
        
        # Sort code prefix matches by code length
        code_prefix_matches.sort(key=lambda x: len(x.code))
        for node in code_prefix_matches[:max_results//2]:
            if node.to_dict() not in results:  # Avoid duplicates
                results.append(node.to_dict())
        
        # If we still have room, search by title
        if len(results) < max_results:
            title_query = query
            # If query was a numeric code, make title search more flexible
            if query.isdigit() and len(query) <= 6:
                title_query = ""  # This will match everything
                
            matches = self.naics_data[
                self.naics_data['Title'].str.lower().str.contains(title_query, na=False)
            ]
            
            # Convert matches to nodes
            for _, row in matches.iterrows():
                if len(results) >= max_results:
                    break
                    
                code = str(row['Code']).strip()
                node = self.get_node(code)
                if node and node.to_dict() not in results:
                    results.append(node.to_dict())
        
        return results
    
    def get_industry_sectors(self):
        """
        Get the top-level industry sectors (2-digit NAICS codes).
        
        Returns:
            list: List of top-level industry sectors in dictionary format.
        """
        sectors = []
        for code, node in self.root.children.items():
            if len(code.replace('-', '')) <= 2:  # Top-level sectors are 2-digit codes
                sectors.append(node.to_dict())
        
        # Sort by code
        sectors.sort(key=lambda x: x['code'])
        return sectors
    
    def get_code_path(self, code):
        """
        Get the full hierarchical path for a NAICS code.
        
        Args:
            code (str): NAICS code to query.
            
        Returns:
            list: List of dictionaries containing the hierarchical path
                 from sector to the specified code.
        """
        node = self.get_node(code)
        if not node:
            return []
        
        path = node.get_path()
        return [n.to_dict() for n in path if n.code != "ROOT"]
    
    def compare_codes(self, code1, code2):
        """
        Compare two NAICS codes to find their common ancestry and differences.
        
        Args:
            code1 (str): First NAICS code.
            code2 (str): Second NAICS code.
            
        Returns:
            dict: Information about common ancestry and differences.
        """
        node1 = self.get_node(code1)
        node2 = self.get_node(code2)
        
        if not node1 or not node2:
            missing = []
            if not node1:
                missing.append(code1)
            if not node2:
                missing.append(code2)
            return {'error': f"Code(s) not found: {', '.join(missing)}"}
        
        path1 = node1.get_path()
        path2 = node2.get_path()
        
        # Find common ancestry
        common = []
        for i in range(min(len(path1), len(path2))):
            if path1[i].code == path2[i].code:
                common.append(path1[i].to_dict())
            else:
                break
        
        # Get the unique parts of each path
        unique1 = [n.to_dict() for n in path1[len(common):]]
        unique2 = [n.to_dict() for n in path2[len(common):]]
        
        # Get the most specific common ancestor
        most_specific_common = common[-1] if common else None
        
        return {
            'common_ancestor': most_specific_common,
            'common_path': common,
            'unique_to_code1': unique1,
            'unique_to_code2': unique2,
            'relationship_distance': len(unique1) + len(unique2)
        }
    
    def to_dict(self):
        """
        Export the whole NAICS tree as a dictionary.
        
        Returns:
            dict: Dictionary representation of the NAICS tree
        """
        def node_to_dict(node):
            result = {
                'code': node.code,
                'title': node.title,
                'is_range': node.is_range,
                'alternate_codes': node.alternate_codes,
                'children': {}
            }
            
            for child_code, child in node.children.items():
                result['children'][child_code] = node_to_dict(child)
                
            return result
        
        return node_to_dict(self.root)
    
    def export_to_json(self, filename=Config.NAICS_HIERARCHY_JSON_PATH):
        """
        Export the NAICS tree to a JSON file.
        
        Args:
            filename (str): Path to save the JSON file
        """
        import json
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)