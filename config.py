import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

class Config:
    """Base configuration class."""
    # Set the base directory for the application
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Set the path to the excel file directory
    EXCEL_FILE_DIR = os.path.join(BASE_DIR, 'itac_excel_files')
    DATABASE_FILE_DIR = os.path.join(BASE_DIR, 'itac_database_files')

    # ITAC Database State
    DATETIME_STATE = "20250407" # YYYYMMDD format, abstract this in production
    ITAC_DATABASE_WEBSITE = "https://itac.university/download"
    ITAC_DATABASE_PATH = os.path.join(EXCEL_FILE_DIR, f'ITAC_Database_{DATETIME_STATE}.xls')

    # 2022 Codes for NAICS, most recent update
    # https://www.census.gov/naics/?48967
    CENSUS_YEAR = "2022"
    NAICS_CODES_PATH = os.path.join(EXCEL_FILE_DIR, f'NAICS_Codes.xlsx')

    # ARC Data Details, from teams
    ARC_LIST_PATH = os.path.join(EXCEL_FILE_DIR, 'ARC_Codes.xlsx')

    ITAC_DATABASE_SQL_PATH = os.path.join(DATABASE_FILE_DIR, f'ITAC_Database_{DATETIME_STATE}.db')
    NAICS_HIERARCHY_JSON_PATH = os.path.join(DATABASE_FILE_DIR, 'naics_hierarchy.json')
    ARC_JSON_PATH = os.path.join(DATABASE_FILE_DIR, 'arc_hierarchy.json')