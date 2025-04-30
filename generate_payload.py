import os
import requests
import re
import json
import traceback
import sqlite3
from config import Config
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Get directory paths
ITAC_DB = Config.ITAC_DATABASE_SQL_PATH
NAICS_DB = Config.NAICS_HIERARCHY_JSON_PATH
ARC_DB = Config.ARC_JSON_PATH

def generate_entire_payload(arc_data, naics_data):
    # Connect to SQLite database
    conn = sqlite3.connect(ITAC_DB)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    cursor = conn.cursor()
    
    # Query for all recommendations with their assessment data,
    # now including center and state
    query = """
    SELECT 
        r.arc, 
        r.assessment_id, 
        r.imp_status, 
        r.imp_cost, 
        r.fiscal_year,
        a.center,
        a.state,
        r.total_savings,
        r.p_conserved_mmbtu,
        r.total_energy_saved,
        a.naics,
        a.products
    FROM 
        recommendations r
    JOIN 
        assessments a ON r.assessment_id = a.id
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Format the results as specified
    formatted_results = []
    for row in results:
        formatted_result = {
            "number_arc": row["arc"],
            "number_naics": row["naics"],
            "description_naics": get_naics_description(row["naics"], naics_data),
            "description_arc": get_arc_description(row["arc"], arc_data),
            "product_naics": row["products"],
            
            # newly added fields:
            "center":       row["center"],
            "state":        row["state"],
            "fiscal_year":  row["fiscal_year"],
            
            "implemented":  row["imp_status"] == "I",
            "cost":         row["imp_cost"],
            "total_savings":       row["total_savings"],
            "p_conserved_mmbtu":   row["p_conserved_mmbtu"],
            "energy_savings":      row["total_energy_saved"],
        }
        formatted_results.append(formatted_result)
    
    conn.close()
    return formatted_results

# ──────────────────────────────────────────────────────────────────────────────
def get_arc_description(code: str | int, arc_data: dict) -> str:
    return arc_data.get("arc_codes", {}).get(str(code), "ARC description not found")
# ──────────────────────────────────────────────────────────────────────────────
def _dollar(v: float | int | None) -> str:
    return f"${(v or 0):,.0f}"

def _percent(v: float | int | None) -> str:
    return f"{(v or 0):.1f}%"
# ──────────────────────────────────────────────────────────────────────────────

def _currency(val: float) -> str:
    return f"${val:,.0f}"

def _percent(val: float, places: int = 1) -> str:
    return f"{val:.{places}f}%"

def _safe(val, places: int):
    return round(val, places) if val is not None else 0


def get_naics_description(naics_code, naics_data):
    """Find the description for a NAICS code in the hierarchy"""
    if not naics_code:
        return "Unknown"
    
    # Convert to string and handle decimal points
    try:
        naics_str = str(int(float(naics_code)))
    except (ValueError, TypeError):
        naics_str = str(naics_code)
    
    # Search through the NAICS hierarchy
    def search_code(node, code):
        # Check if this node matches the code
        if node.get("code") == code:
            return node.get("title")
        
        # Check children
        if "children" in node:
            # Direct lookup in children dictionary
            if code in node["children"]:
                return node["children"][code].get("title")
            
            # Recursive search through children
            for child_code, child in node["children"].items():
                if code.startswith(child_code):
                    result = search_code(child, code)
                    if result:
                        return result
        
        return None
    
    description = search_code(naics_data, naics_str)
    return description if description else "NAICS description not found"

def get_arc_description(arc_code, arc_data):
    if not arc_code:
        return "Unknown"

    return arc_data.get("arc_codes", {}).get(str(arc_code), "ARC description not found")

def get_arc_data_by_precision(arc_code, arc_data):
    """
    Find and return data from the ARC hierarchy based on a specific ARC code.
    
    Parameters:
    - arc_code (str): The ARC code to search for (e.g., "2.1", "2.11", "2.111", etc.)
    - arc_data (dict): The JSON data containing the ARC hierarchy
    
    Returns:
    - dict: The subset of ARC data corresponding to the provided code.
    """

    arc_code = str(arc_code).strip()

    try:
        hierarchy = arc_data['arc_hierarchy']

        arc_code_digits = [digit for digit in arc_code if digit.isdigit()]
        arc_category = arc_code_digits[0] if arc_code_digits else None
        arc_code_digits = arc_code_digits[1:] if len(arc_code_digits) > 1 else None

        level = hierarchy[arc_category]
        
        if arc_category and not arc_code_digits:
            print("No digits found in arc_code, returning level")
            return level
        

        level = level.get('children', {})
        arc_category += '.'

        for digit in arc_code_digits:
            arc_category += digit

            if arc_code in level:
                return level[arc_code]
            
            level = level[arc_category].get('children', {})


    except (KeyError, IndexError):
        return {}
    
def extract_code_descriptions(data, results=None):
    if results is None:
        results = {}
    
    # Check if this is a dictionary with code and description
    if isinstance(data, dict) and "code" in data and "description" in data:
        # Add this code-description pair to results
        results[data["code"]] = data["description"]
        
        # If there are children, process them too
        if "children" in data and isinstance(data["children"], dict):
            for child_key, child_data in data["children"].items():
                extract_code_descriptions(child_data, results)
    
    # If data is a dictionary but not the expected format, check all values
    elif isinstance(data, dict):
        for key, value in data.items():
            extract_code_descriptions(value, results)
            
    return results

def generate_top_recommendations(arc_precision=None, fiscal_year=None):
    fiscal_year_range = []

    if type(fiscal_year) == int:
        fiscal_year_range = [fiscal_year]
    elif type(fiscal_year) == list:
        fiscal_year_range = fiscal_year

    with open(NAICS_DB, 'r') as f:
        naics_data = json.load(f)

    with open(ARC_DB, 'r') as f:
        arc_data = json.load(f)

    
    if not arc_precision:
        recomendation_load = arc_data.copy()
    else:
        recomendation_load = get_arc_data_by_precision(arc_precision, arc_data) if arc_precision else None

    sectioned_arc_recomendations = extract_code_descriptions(recomendation_load)
    
    recomendations = {}
    
    conn = sqlite3.connect(ITAC_DB)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    cursor = conn.cursor()
    
    # Get all recommendations from the database
    query = "SELECT * FROM recommendations"
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Process each row
    for row in rows:
        arc_code = row['arc']
        
        # Skip if arc_code is None
        if arc_code is None:
            continue
        
        # Convert arc_code to string for dictionary lookup
        arc_key = str(arc_code)
        
        # Skip if arc_key not in sectioned_arc_recommendations
        if arc_key not in sectioned_arc_recomendations:
            print(f"ARC code {arc_key} not found in sectioned recommendations.")
            continue
        
        print(f"Processing ARC code: {arc_key}")

        # Check fiscal year if enabled
        if fiscal_year_range and row['fiscal_year'] not in fiscal_year_range:
            continue
        
        # Get values from the row, replacing nulls with 0
        savings = row['total_savings'] if row['total_savings'] is not None else 0
        payback = row['payback'] if row['payback'] is not None else 0
        cost = row['imp_cost'] if row['imp_cost'] is not None else 0
        implementation_status = 1 if row['imp_status'] == 'I' else 0

        # Get or create the recommendation entry
        if arc_key not in recomendations:
            recomendations[arc_key] = {
                'description': sectioned_arc_recomendations[arc_key],
                'savings': [],
                'payback': [],
                'cost': [],
                'implementation_status': []
            }
        
        # Add values to the arrays
        recomendations[arc_key]['savings'].append(savings)
        recomendations[arc_key]['payback'].append(payback)
        recomendations[arc_key]['cost'].append(cost)
        recomendations[arc_key]['implementation_status'].append(implementation_status)
    
    conn.close()
    
    # Generate the final result dictionary
    result = {}
    for arc_code, data in recomendations.items():
        savings_array = data['savings']
        cost_array = data['cost']
        payback_array = data['payback']
        implementation_status_array = data['implementation_status']
        
        # Calculate averages and implementation rate
        avg_savings = sum(savings_array) / len(savings_array) if savings_array else 0
        avg_cost = sum(cost_array) / len(cost_array) if cost_array else 0
        avg_payback = sum(payback_array) / len(payback_array) if payback_array else 0
        
        implementation_count = sum(implementation_status_array)
        total_count = len(implementation_status_array)
        implementation_rate = (implementation_count / total_count * 100) if total_count > 0 else 0
        implementation_rate = round(implementation_rate, 1)  # Round to 1 decimal place
        
        # Add to result
        result[arc_code] = {
            'arc_code': arc_code,
            'description': data['description'],
            'average_savings': avg_savings,
            'average_cost': avg_cost,
            'average_payback': avg_payback,
            'implementation_rate': implementation_rate,
            'times_recommended': len(savings_array)
        }
    
    return result    

@app.route('/arc/<arc_code>', methods=['GET'])
def get_arc_subset(arc_code):
    try:
        # Load ARC data
        with open(ARC_DB, 'r') as f:
            arc_data = json.load(f)
        
        
        # Get the subset of ARC data for the requested code
        result = get_arc_data_by_precision(arc_code, arc_data)
        
        if result and isinstance(result, dict):
            # If the result has children, we need to format it
            return jsonify({arc_code: {"code": arc_code, "description": result.get("description", "No description available"), "children": result["children"]}} ), 200
            
        else:
            return jsonify({"error": "No data found for the given ARC code"}), 404
            
    except Exception as e:
        return jsonify({"error": str(e), "trace": str(traceback.format_exc())}), 500

@app.route('/recomendations', methods=['GET'])   
def get_top_recommendations():
    try:
        # Get parameters from the request
        arc_precision = request.args.get('arc_precision')
        fiscal_year = request.args.get('fiscal_year')

        # Convert fiscal_year to int or list of ints
        if fiscal_year:
            if ',' in fiscal_year:
                fiscal_year = [int(year.strip()) for year in fiscal_year.split(',')]
            else:
                fiscal_year = int(fiscal_year)
        
        # Convert arc_precision to appropriate type if needed
        if arc_precision and arc_precision.isdigit():
            arc_precision = int(arc_precision)
        
        # Generate top recommendations
        recommendations = generate_top_recommendations(arc_precision, fiscal_year)
        
        return jsonify({"success": True, "data": recommendations}), 200
    
    except ValueError as e:
        # Handle parameter conversion errors
        return jsonify({"success": False, "error": f"Invalid parameter: {str(e)}"}), 400
    
    except Exception as e:
        # Handle any other errors
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": str(e), "trace": str(traceback.format_exc())}), 500

@app.route('/all')
def get_all_data():
    try:
        # Load NAICS hierarchy
        with open(NAICS_DB, 'r') as f:
            naics_data = json.load(f)

        with open(ARC_DB, 'r') as f:
            arc_data = json.load(f)
        
        formatted_results = generate_entire_payload(arc_data, naics_data)
        
        return jsonify(formatted_results)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/preview', methods=['GET'])
def preview_data():
    """
    Quick‐look endpoint: loads just the first 20 recommendations
    so you can open it in your browser without choking.
    """
    try:
        # load hierarchies
        with open(NAICS_DB, 'r') as f:
            naics_data = json.load(f)
        with open(ARC_DB, 'r') as f:
            arc_data = json.load(f)

        # grab everything, then slice
        all_data = generate_entire_payload(arc_data, naics_data)
        sample = all_data[:20]

        return jsonify(sample), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

import re

@app.route("/aggregates", methods=["GET"])
def get_aggregates_by_arc():
    try:
        center   = request.args.get("center")
        state    = request.args.get("state")
        fy_param = request.args.get("fiscal_year")
        arc_code = request.args.get("arc")

        where, params = [], []
        if center:
            where.append("a.center = ?");   params.append(center)
        if state:
            where.append("a.state  = ?");   params.append(state)

        # ── FIX: filter on recommendation year (r.fiscal_year), not a.fiscal_year ──
        if fy_param:
            m = re.match(r"^\s*(<=|>=|=)?\s*(\d{4})\s*$", fy_param)
            if not m:
                return jsonify({
                    "success": False,
                    "error": "Bad fiscal_year (ex: =2023 | >=2020 | <=2018)"
                }), 400

            op, yr = m.group(1) or "=", int(m.group(2))
            where.append(f"r.fiscal_year {op} ?")
            params.append(yr)

        # prefix‐match ARC codes as before
        if arc_code:
            arc_code = arc_code.strip()
            where.append("r.arc LIKE ?")
            params.append(f"{arc_code}%")

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        query = f"""
        SELECT r.arc                                   AS arc_code,
            AVG(r.total_savings)                    AS average_savings,
            -- implemented OR blank rows only:
          CASE WHEN SUM(CASE WHEN (r.imp_status='I' OR r.imp_status IS NULL
                                        OR r.imp_status='' OR r.imp_status='N') THEN 1 END)=0
                THEN 0
                ELSE SUM(CASE WHEN (r.imp_status='I' OR r.imp_status IS NULL
                                        OR r.imp_status='' OR r.imp_status='N') THEN r.imp_cost END)*1.0
                        / SUM(CASE WHEN (r.imp_status='I' OR r.imp_status IS NULL
                                        OR r.imp_status='' OR r.imp_status='N') THEN 1 END)
          END                                     AS average_cost,
          CASE 
                WHEN COUNT(*)=0 THEN 0
                ELSE CAST(
                    AVG(
                        CASE
                        WHEN r.p_saved > 0
                            THEN CAST((COALESCE(r.imp_cost,0)*1.0/r.p_saved)*10 AS INTEGER)/10.0
                        ELSE NULL
                        END
                    ) * 10
                    AS INTEGER
                    ) / 10.0
            END                                  AS average_payback,
          CASE 
                WHEN SUM(CASE WHEN r.imp_status IN ('I','N') THEN 1 END)=0 
                THEN 0 
                ELSE SUM(CASE WHEN r.imp_status='I' THEN 1 END)*100.0
                    / SUM(CASE WHEN r.imp_status IN ('I','N') THEN 1 END)
            END                                 AS implementation_rate,
          COUNT(*)                                 AS times_recommended
        FROM   recommendations r
        JOIN   assessments     a ON r.assessment_id = a.id
        {where_sql}
        GROUP BY r.arc
        ORDER BY r.arc;
        """

        conn  = sqlite3.connect(ITAC_DB)
        conn.row_factory = sqlite3.Row
        rows  = conn.execute(query, params).fetchall()
        conn.close()

        with open(ARC_DB) as f:
            arc_data = json.load(f)

        payload = []
        for r in rows:
            code = r["arc_code"]
            payload.append({
              "arc":               code,
              "description":       get_arc_description(code, arc_data),
              "avgSavings":        _currency(_safe(r["average_savings"],  0)),
              "avgCost":           _currency(_safe(r["average_cost"],      0)),
              "avgPayback":        round(_safe(r["average_payback"], 2), 2),
              "implementationRate":_percent(_safe(r["implementation_rate"], 1)),
              "recommended":       r["times_recommended"]
            })

        return jsonify({"success": True, "data": payload}), 200

    except Exception as e:
        return jsonify({
          "success": False,
          "error": str(e),
          "trace": traceback.format_exc()
        }), 500


@app.route("/filter-options", methods=["GET"])
def get_filter_options():
    try:
        conn = sqlite3.connect(ITAC_DB)
        cur  = conn.cursor()

        cur.execute("SELECT DISTINCT center       FROM assessments WHERE center IS NOT NULL")
        centers = sorted(row[0] for row in cur.fetchall())

        cur.execute("SELECT DISTINCT state        FROM assessments WHERE state  IS NOT NULL")
        states  = sorted(row[0] for row in cur.fetchall())

        cur.execute("SELECT DISTINCT fiscal_year  FROM assessments WHERE fiscal_year IS NOT NULL")
        years   = sorted((row[0] for row in cur.fetchall()), reverse=True)

        conn.close()
        return jsonify({
            "success": True,
            "centers": centers,
            "states":  states,
            "years":   years
        }), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)