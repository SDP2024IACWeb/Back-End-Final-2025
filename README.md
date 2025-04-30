# ITAC Database API

A simple Flask-based API for accessing data from the ITAC (Industrial Technical Assistance Center) database.

## Overview

This application provides a REST API endpoint to retrieve formatted data from the ITAC SQLite database. It joins the `recommendations` and `assessments` tables, and combines the data with NAICS (North American Industry Classification System) descriptions from the provided hierarchy JSON file.

## Features

- Single `/all` endpoint that returns all recommendations with associated assessment data
- Automatic lookup of NAICS descriptions from the hierarchy
- JSON formatted response with all necessary fields
- Error handling for database and file operations

## Dependencies

- Python 3.6+
- Flask
- SQLite3 (included in Python standard library)

## Installation

1. Clone this repository
2. Ensure your database files are in the correct location
   ```
   /your-app-directory
   ├── app.py
   └── database-files
       ├── ITAC_Database_20250407.sql
       └── naics_hierarchy.json
   ```
3. Install dependencies:
   ```
   pip install flask
   ```

## Usage

1. Start the Flask application:
   ```
   python app.py
   ```

2. Access the API endpoint at:
   ```
   http://localhost:5000/all
   ```

## API Response Format

The API returns an array of objects, where each object represents a row from the recommendations table joined with its corresponding assessment data:

```json
[
  {
    "number_arc": "value",
    "number_naics": "value",
    "description_naics": "NAICS description from hierarchy",
    "product_naics": "product description",
    "fiscal_year": "value",
    "implemented": true/false,
    "cost": "value",
    "total_savings": "value",
    "p_conserved_mmbtu": "value",
    "energy_savings": "value"
  },
  ...
]
```

### Field Descriptions

- `number_arc`: The ARC value from the recommendations table
- `number_naics`: NAICS code from the assessments table
- `description_naics`: Descriptive text for the NAICS code from the hierarchy
- `product_naics`: Product description from the assessments table
- `fiscal_year`: Fiscal year from the recommendations table
- `implemented`: Boolean indicating if implementation status is "I"
- `cost`: Implementation cost from the recommendations table
- `total_savings`: Total savings from the recommendations table
- `p_conserved_mmbtu`: Primary energy conserved in MMBTU
- `energy_savings`: Total energy saved from the recommendations table

## Error Handling

If an error occurs during processing, the API will return a JSON object with an error message:

```json
{
  "error": "Error message description"
}
```

## Performance Considerations

- For large databases, consider implementing pagination
- The API loads all data into memory, which may be inefficient for very large datasets
- The NAICS lookup function performs recursive searches which may be slow for deep hierarchies

## Future Enhancements

- Add pagination support
- Add filtering options
- Implement caching for NAICS lookups
- Add authentication