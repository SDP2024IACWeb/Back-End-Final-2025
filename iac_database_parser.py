"""
IAC Database Parser Module

This module provides functionality for loading and analyzing data from the 
Industrial Assessment Center (IAC) database, with a focus on the University 
of Connecticut (UConn) IAC recommendations.

The IAC database contains assessment data (ASSESS tab) and recommendation data 
(RECC tabs) for various industrial assessments. This parser specifically 
extracts and analyzes the recommendations associated with UConn.

For more information about the IAC database structure and terminology, see:
https://iac.university/#database
https://iac.university/recommendationTypes
"""

"""

TODO in convert_to_database:
        # Process ARC hierarchy
        # Create basic ARC hierarchy from recommendation data
        # Process NAICS codes
        # Use the existing NAICS parser for industry code0s


        # Extract unique NAICS codes
        
        # Insert industry codes
        

"""


import os
import sqlite3
import numpy as np
import pandas as pd
from config import Config
from datetime import datetime
from parse_xls import parse_xls
from naics_parser import NAICSParser


class IACDatabaseParser:
    """
    Parser for the Industrial Assessment Center (IAC) database.
    
    This class provides methods to load, filter, and analyze data from the IAC database,
    with specific focus on UConn (University of Connecticut) recommendations.
    """
    
    # Define constants for easier maintenance
    REC_RANGE = range(1, 4)  # RECC1 through RECCX tabs
    RECOMMENDATION_TAG = 'UC'  # UConn identifier
    
    # Classification of recommendation implementation status
    STATUS_CODES = {
        'I': 'Implemented', 
        'P': 'Pending',
        'N': 'Not Implemented',
        'R': 'Rejected',
        'X': 'Not Applicable',
        '?': 'Unknown'
    }
    
    # Energy resource mappings for various statistics fields
    RESOURCE_MAPPINGS = {
        'P': {'CONSERVED': 'PCONSERVED', 'SAVED': 'PSAVED', 'UNIT': 'kWh', 'LABEL': 'Electricity'},
        'S': {'CONSERVED': 'SCONSERVED', 'SAVED': 'SSAVED', 'UNIT': 'MMBTU', 'LABEL': 'Natural Gas'},
        'T': {'CONSERVED': 'TCONSERVED', 'SAVED': 'TSAVED', 'UNIT': 'MMBTU', 'LABEL': 'Other Resource'},
        'Q': {'CONSERVED': 'QCONSERVED', 'SAVED': 'QSAVED', 'UNIT': 'kGal', 'LABEL': 'Water'}
    }

    # Key statistical fields to extract from the recommendations
    # These fields include fiscal year, implementation cost, energy savings, payback period, and various other financial and resource metrics.
    KEY_STATISTICS = [
            "FY", "IMPCOST", "PCONSERVED", "PSAVED", "SCONSERVED", "SSAVED", 
            "TCONSERVED", "TSAVED", "QCONSERVED", "QSAVED", "PAYBACK", 
            "IC_CAPITAL", "IC_OTHER", "REBATE", "INCREMNTAL", "BPTOOL", "ARC"
    ]
    
    def __init__(self):
        """Initialize the IAC database parser."""
        self.database_path = Config.ITAC_DATABASE_PATH
        self.database_items = None
        self.uconn_recommendations = None
        self.uconn_naics_mapping = NAICSParser()
    
    def load_database(self):
        """
        Load the complete IAC database from the specified path.
        
        Returns:
            dict: A dictionary containing DataFrames for each sheet in the IAC Database.
        
        Raises:
            FileNotFoundError: If the database file cannot be found.
        """
        self.database_items = parse_xls(self.database_path)
        
        if self.database_items is None:
            raise FileNotFoundError(f"Could not find the IAC database file at {self.database_path}")
        
        return self.database_items
    
    def load_uconn_recommendations(self):
        """
        Load and combine all UConn recommendations from the IAC database.
        
        This method extracts recommendation data from all RECC tabs in the 
        database and filters it to only include UConn recommendations.
        
        Returns:
            pandas.DataFrame: Combined DataFrame of all UConn recommendations.
        """
        if self.database_items is None:
            self.load_database()
        
        # Extract all recommendation dataframes
        recc_dfs = [pd.DataFrame(self.database_items[f'RECC{i}']) for i in self.REC_RANGE]
        
        # Filter for UConn recommendations
        # uconn_recc_dfs = [df[df['SUPERID'].str.startswith(self.RECOMMENDATION_TAG)] for df in recc_dfs]
        
        # Combine all UConn recommendation dataframes
        self.uconn_recommendations = pd.concat(recc_dfs, ignore_index=True)
        
        return self.uconn_recommendations
    
    def load_uconn_assessments(self):
        """`
        Load and filter UConn assessments from the IAC database.
        
        This method extracts assessment data from the ASSESS tab in the
        database and filters it to only include UConn assessments.
        
        Returns:
            pandas.DataFrame: DataFrame containing only UConn assessments.
        
        """

        if self.database_items is None:
            self.load_database()
        
        # Extract assessment dataframes
        assess_dfs = pd.DataFrame(self.database_items[f'ASSESS'])

        # Filter for UConn assessments
        # uconn_assess_dfs = assess_dfs[assess_dfs['CENTER'].str.startswith(self.RECOMMENDATION_TAG)]

        return assess_dfs
    

    def convert_to_database(self, df=None, output_db_path=None):
        """
        Convert IAC database data to SQL database format.
        
        This function loads the UConn IAC assessment and recommendation data,
        processes it, and stores it in a SQLite database suitable for
        dashboard queries and visualizations.
        
        Args:
            df (pandas.DataFrame, optional): Optional pre-loaded DataFrame.
                If None, loads from the IAC database.
            output_db_path (str, optional): Path to output SQL database.
                If None, uses Config.ITAC_DATABASE_SQL_PATH
                
        Returns:
            dict: Information about the conversion process
        """

        # Set default output path if not specified
        if output_db_path is None:
            output_db_path = getattr(Config, 'ITAC_DATABASE_SQL_PATH')
        
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(output_db_path), exist_ok=True)
        
        # Load data if not already loaded
        if self.database_items is None:
            self.load_database()
        
        # Create database connection
        conn = sqlite3.connect(output_db_path)
        cursor = conn.cursor()
        
        # Create tables (schema definition)
        cursor.executescript('''
        -- Assessments table (from ASSESS tab)
        CREATE TABLE IF NOT EXISTS assessments (
            id VARCHAR(20) PRIMARY KEY,
            center VARCHAR(10),
            fiscal_year INT,
            sic VARCHAR(10),
            naics VARCHAR(20),
            state VARCHAR(2),
            sales DECIMAL(20,2),
            employees INT,
            plant_area DECIMAL(20,2),
            products TEXT,
            prod_units VARCHAR(50),
            prod_level DECIMAL(20,2),
            prod_hours DECIMAL(20,2),
            num_recommendations INT,
            
            -- Energy consumption data
            electricity_cost DECIMAL(20,2),
            electricity_usage DECIMAL(20,2),
            electricity_demand_cost DECIMAL(20,2),
            electricity_demand_usage DECIMAL(20,2),
            electricity_fees DECIMAL(20,2),
            natural_gas_cost DECIMAL(20,2),
            natural_gas_usage DECIMAL(20,2),
            lpg_cost DECIMAL(20,2),
            lpg_usage DECIMAL(20,2),
            fuel_oil1_cost DECIMAL(20,2),
            fuel_oil1_usage DECIMAL(20,2),
            fuel_oil2_cost DECIMAL(20,2),
            fuel_oil2_usage DECIMAL(20,2),
            fuel_oil4_cost DECIMAL(20,2),
            fuel_oil4_usage DECIMAL(20,2),
            fuel_oil6_cost DECIMAL(20,2),
            fuel_oil6_usage DECIMAL(20,2),
            coal_cost DECIMAL(20,2),
            coal_usage DECIMAL(20,2),
            wood_cost DECIMAL(20,2),
            wood_usage DECIMAL(20,2),
            paper_cost DECIMAL(20,2),
            paper_usage DECIMAL(20,2),
            other_gas_cost DECIMAL(20,2),
            other_gas_usage DECIMAL(20,2),
            other_energy_cost DECIMAL(20,2),
            other_energy_usage DECIMAL(20,2),
            
            -- Total energy metrics (computed)
            total_energy_cost DECIMAL(20,2),
            total_energy_usage DECIMAL(20,2),
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Recommendations table (from RECC tabs)
        CREATE TABLE IF NOT EXISTS recommendations (
            super_id VARCHAR(30) PRIMARY KEY,
            assessment_id VARCHAR(20),
            rec_number INT,
            app_code VARCHAR(10),
            arc VARCHAR(20),
            imp_status VARCHAR(1),
            imp_cost DECIMAL(20,2),
            
            -- Primary resource (electricity)
            p_source_code VARCHAR(10),
            p_conserved DECIMAL(20,2),
            p_source_conserved DECIMAL(20,2),
            p_saved DECIMAL(20,2),
            
            -- Secondary resource (natural gas)
            s_source_code VARCHAR(10),
            s_conserved DECIMAL(20,2),
            s_source_conserved DECIMAL(20,2),
            s_saved DECIMAL(20,2),
            
            -- Tertiary resource (other)
            t_source_code VARCHAR(10),
            t_conserved DECIMAL(20,2),
            t_source_conserved DECIMAL(20,2),
            t_saved DECIMAL(20,2),
            
            -- Quaternary resource (water)
            q_source_code VARCHAR(10),
            q_conserved DECIMAL(20,2),
            q_source_conserved DECIMAL(20,2),
            q_saved DECIMAL(20,2),
            
            rebate VARCHAR(1),
            incremental VARCHAR(1),
            fiscal_year INT,
            ic_capital DECIMAL(20,2),
            ic_other DECIMAL(20,2),
            payback DECIMAL(10,2),
            bp_tool VARCHAR(50),
            
            -- Computed fields
            total_savings DECIMAL(20,2),
            total_energy_saved DECIMAL(20,2),
            
            FOREIGN KEY (assessment_id) REFERENCES assessments(id)
        );

        -- Resource codes reference table
        CREATE TABLE IF NOT EXISTS resource_codes (
            code VARCHAR(10) PRIMARY KEY,
            description VARCHAR(100),
            unit VARCHAR(20),
            energy_type VARCHAR(30),
            conversion_factor DECIMAL(20,6)
        );
        ''')
        
        # Process and insert assessments data
        assessments_df = self.load_uconn_assessments()
        
        # Clean and transform assessments data
        assessment_columns = {
            'ID': 'id',
            'CENTER': 'center',
            'FY': 'fiscal_year',
            'SIC': 'sic',
            'NAICS': 'naics',
            'STATE': 'state',
            'SALES': 'sales',
            'EMPLOYEES': 'employees',
            'PLANT_AREA': 'plant_area',
            'PRODUCTS': 'products',
            'PRODUNITS': 'prod_units',
            'PRODLEVEL': 'prod_level',
            'PRODHOURS': 'prod_hours',
            'NUMARS': 'num_recommendations',
            'EC_plant_cost': 'electricity_cost',
            'EC_plant_usage': 'electricity_usage',
            'ED_plant_cost': 'electricity_demand_cost',
            'ED_plant_usage': 'electricity_demand_usage',
            'EF_plant_cost': 'electricity_fees',
            'E2_plant_cost': 'natural_gas_cost',
            'E2_plant_usage': 'natural_gas_usage',
            'E3_plant_cost': 'lpg_cost',
            'E3_plant_usage': 'lpg_usage',
            'E4_plant_cost': 'fuel_oil1_cost',
            'E4_plant_usage': 'fuel_oil1_usage',
            'E5_plant_cost': 'fuel_oil2_cost',
            'E5_plant_usage': 'fuel_oil2_usage',
            'E6_plant_cost': 'fuel_oil4_cost',
            'E6_plant_usage': 'fuel_oil4_usage',
            'E7_plant_cost': 'fuel_oil6_cost',
            'E7_plant_usage': 'fuel_oil6_usage',
            'E8_plant_cost': 'coal_cost',
            'E8_plant_usage': 'coal_usage',
            'E9_plant_cost': 'wood_cost',
            'E9_plant_usage': 'wood_usage',
            'E10_plant_cost': 'paper_cost',
            'E10_plant_usage': 'paper_usage',
            'E11_plant_cost': 'other_gas_cost',
            'E11_plant_usage': 'other_gas_usage',
            'E12_plant_cost': 'other_energy_cost',
            'E12_plant_usage': 'other_energy_usage'
        }
        
        # Rename columns 
        clean_assess_df = assessments_df.rename(columns=assessment_columns)
        
        # Filter for relevant columns
        clean_assess_df = clean_assess_df[[col for col in assessment_columns.values() if col in clean_assess_df.columns]]
        
        # Add computed columns
        # Calculate total energy cost
        energy_cost_cols = [
            'electricity_cost', 'electricity_demand_cost', 'electricity_fees',
            'natural_gas_cost', 'lpg_cost', 
            'fuel_oil1_cost', 'fuel_oil2_cost', 'fuel_oil4_cost', 'fuel_oil6_cost',
            'coal_cost', 'wood_cost', 'paper_cost', 'other_gas_cost', 'other_energy_cost'
        ]
        clean_assess_df['total_energy_cost'] = clean_assess_df[
            [col for col in energy_cost_cols if col in clean_assess_df.columns]
        ].sum(axis=1, skipna=True)
        
        # Calculate total energy usage (all in MMBtu)
        # Convert electricity from kWh to MMBtu (3412 BTU/kWh)
        if 'electricity_usage' in clean_assess_df.columns:
            clean_assess_df['electricity_usage_mmbtu'] = clean_assess_df['electricity_usage'] * 3412 / 1000000
        else:
            clean_assess_df['electricity_usage_mmbtu'] = 0
        
        # Sum all energy usage
        energy_usage_cols = [
            'natural_gas_usage', 'lpg_usage', 
            'fuel_oil1_usage', 'fuel_oil2_usage', 'fuel_oil4_usage', 'fuel_oil6_usage',
            'coal_usage', 'wood_usage', 'paper_usage', 'other_gas_usage', 'other_energy_usage'
        ]

        clean_assess_df['total_energy_usage'] = clean_assess_df['electricity_usage_mmbtu'] + clean_assess_df[
            [col for col in energy_usage_cols if col in clean_assess_df.columns]
        ].sum(axis=1, skipna=True)
        
        # Clean up data types
        numeric_cols = [col for col in clean_assess_df.columns if col not in ['id', 'center', 'sic', 'naics', 'state', 'products', 'prod_units']]
        for col in numeric_cols:
            clean_assess_df[col] = pd.to_numeric(clean_assess_df[col], errors='coerce')
        
        # Insert assessments data
        clean_assess_df.to_sql('assessments', conn, if_exists='replace', index=False)
        
        # Process and insert recommendations data
        if self.uconn_recommendations is None:
            self.load_uconn_recommendations()
        
        recommendations_df = self.uconn_recommendations
        
        # Clean and transform recommendations data
        recommendation_columns = {
            'SUPERID': 'super_id',
            'ID': 'assessment_id',
            'AR_NUMBER': 'rec_number',
            'APPCODE': 'app_code',
            'ARC2': 'arc',
            'IMPSTATUS': 'imp_status',
            'IMPCOST': 'imp_cost',
            'PSOURCCODE': 'p_source_code',
            'PCONSERVED': 'p_conserved',
            'PSOURCONSV': 'p_source_conserved',
            'PSAVED': 'p_saved',
            'SSOURCCODE': 's_source_code',
            'SCONSERVED': 's_conserved',
            'SSOURCONSV': 's_source_conserved',
            'SSAVED': 's_saved',
            'TSOURCCODE': 't_source_code',
            'TCONSERVED': 't_conserved',
            'TSOURCONSV': 't_source_conserved',
            'TSAVED': 't_saved',
            'QSOURCCODE': 'q_source_code',
            'QCONSERVED': 'q_conserved',
            'QSOURCONSV': 'q_source_conserved',
            'QSAVED': 'q_saved',
            'REBATE': 'rebate',
            'INCREMNTAL': 'incremental',
            'FY': 'fiscal_year',
            'IC_CAPITAL': 'ic_capital',
            'IC_OTHER': 'ic_other',
            'PAYBACK': 'payback',
            'BPTOOL': 'bp_tool'
        }
        
        # Rename columns
        clean_recc_df = recommendations_df.rename(columns=recommendation_columns)
        
        # Filter for relevant columns
        clean_recc_df = clean_recc_df[[col for col in recommendation_columns.values() if col in clean_recc_df.columns]]
        
        # Add computed columns
        # Calculate total savings across all resources
        savings_cols = ['p_saved', 's_saved', 't_saved', 'q_saved']
        clean_recc_df['total_savings'] = clean_recc_df[
            [col for col in savings_cols if col in clean_recc_df.columns]
        ].sum(axis=1, skipna=True)
        
        # Calculate total energy saved in MMBtu
        # Convert primary (electricity) from kWh to MMBtu
        if 'p_conserved' in clean_recc_df.columns:
            clean_recc_df['p_conserved_mmbtu'] = clean_recc_df['p_conserved'] * 3412 / 1000000
        else:
            clean_recc_df['p_conserved_mmbtu'] = 0
        
        clean_recc_df['total_energy_saved'] = clean_recc_df['p_conserved_mmbtu']
        
        # Add secondary and tertiary resource savings (already in MMBtu)
        for col in ['s_conserved', 't_conserved']:
            if col in clean_recc_df.columns:
                clean_recc_df['total_energy_saved'] += clean_recc_df[col].fillna(0)
        
        # Clean up data types
        numeric_cols = [col for col in clean_recc_df.columns if col not in [
            'super_id', 'assessment_id', 'app_code', 'arc', 'imp_status', 
            'p_source_code', 's_source_code', 't_source_code', 'q_source_code',
            'rebate', 'incremental', 'bp_tool'
        ]]
        for col in numeric_cols:
            clean_recc_df[col] = pd.to_numeric(clean_recc_df[col], errors='coerce')
        
        # Insert recommendations data
        clean_recc_df.to_sql('recommendations', conn, if_exists='replace', index=False)
        
        # Create resource codes reference data
        resource_codes_data = [
            ('1', 'Electricity', 'kWh', 'Electricity', 0.003412),
            ('2', 'Natural Gas', 'MCF', 'Natural Gas', 1.026),
            ('3', 'Liquefied Petroleum Gas', 'gal', 'LPG', 0.091),
            ('4', '#1 Fuel Oil', 'gal', 'Fuel Oil', 0.139),
            ('5', '#2 Fuel Oil', 'gal', 'Fuel Oil', 0.139),
            ('6', '#4 Fuel Oil', 'gal', 'Fuel Oil', 0.146),
            ('7', '#6 Fuel Oil', 'gal', 'Fuel Oil', 0.15),
            ('8', 'Coal', 'ton', 'Coal', 24.93),
            ('9', 'Wood', 'ton', 'Biomass', 17.2),
            ('10', 'Paper', 'ton', 'Biomass', 15.6),
            ('11', 'Other Gas', 'MCF', 'Gas', 1.0),
            ('12', 'Other Energy', 'MMBtu', 'Other', 1.0),
            ('13', 'Demand', 'kW', 'Electricity', 0.0),
            ('21', 'Water', 'kGal', 'Water', 0.0)
        ]
        
        # Insert resource codes
        resource_codes_df = pd.DataFrame(resource_codes_data, 
                                    columns=['code', 'description', 'unit', 'energy_type', 'conversion_factor'])
        resource_codes_df.to_sql('resource_codes', conn, if_exists='replace', index=False)
    
        # Create views for dashboard queries
        cursor.executescript('''
        -- View for recommendation summary stats
        CREATE VIEW IF NOT EXISTS recommendation_stats AS
        SELECT 
            a.fiscal_year,
            a.state,
            a.naics,
            a.sic,
            r.arc,
            r.imp_status,
            COUNT(*) as rec_count,
            SUM(r.imp_cost) as total_imp_cost,
            SUM(r.total_savings) as total_saved,
            SUM(CASE WHEN r.imp_status = 'I' THEN 1 ELSE 0 END) as implemented_count,
            AVG(r.payback) as avg_payback
        FROM recommendations r
        JOIN assessments a ON r.assessment_id = a.id
        GROUP BY a.fiscal_year, a.state, a.naics, a.sic, r.arc, r.imp_status;

        -- View for energy usage by assessment
        CREATE VIEW IF NOT EXISTS energy_usage AS
        SELECT
            id,
            fiscal_year,
            state,
            naics,
            electricity_usage,
            natural_gas_usage,
            lpg_usage,
            COALESCE(fuel_oil1_usage, 0) + COALESCE(fuel_oil2_usage, 0) + 
            COALESCE(fuel_oil4_usage, 0) + COALESCE(fuel_oil6_usage, 0) as fuel_oil_usage,
            coal_usage,
            COALESCE(wood_usage, 0) + COALESCE(paper_usage, 0) as biomass_usage,
            COALESCE(other_gas_usage, 0) + COALESCE(other_energy_usage, 0) as other_usage,
            total_energy_usage,
            total_energy_cost,
            CASE 
                WHEN total_energy_usage = 0 THEN 0 
                ELSE total_energy_cost / total_energy_usage 
            END as energy_cost_per_mmbtu
        FROM assessments;
        
        -- View for resource savings by type
        CREATE VIEW IF NOT EXISTS resource_savings AS
        SELECT
            r.assessment_id,
            a.fiscal_year,
            a.state,
            a.naics,
            r.arc,
            r.imp_status,
            SUM(r.p_conserved) as electricity_saved_kwh,
            SUM(r.p_saved) as electricity_saved_dollars,
            SUM(r.s_conserved) as natural_gas_saved_mmbtu,
            SUM(r.s_saved) as natural_gas_saved_dollars,
            SUM(r.t_conserved) as other_resource_saved_mmbtu,
            SUM(r.t_saved) as other_resource_saved_dollars,
            SUM(r.q_conserved) as water_saved_kgal,
            SUM(r.q_saved) as water_saved_dollars,
            SUM(r.total_energy_saved) as total_energy_saved_mmbtu,
            SUM(r.total_savings) as total_saved_dollars,
            AVG(r.payback) as avg_payback
        FROM recommendations r
        JOIN assessments a ON r.assessment_id = a.id
        GROUP BY r.assessment_id, a.fiscal_year, a.state, a.naics, r.arc, r.imp_status;
        
        -- View for implementation status summary
        CREATE VIEW IF NOT EXISTS implementation_status AS
        SELECT
            a.fiscal_year,
            r.imp_status,
            CASE
                WHEN r.imp_status = 'I' THEN 'Implemented'
                WHEN r.imp_status = 'P' THEN 'Pending'
                WHEN r.imp_status = 'N' THEN 'Not Implemented'
                WHEN r.imp_status = 'R' THEN 'Rejected'
                WHEN r.imp_status = 'X' THEN 'Not Applicable'
                WHEN r.imp_status = '?' THEN 'Unknown'
                ELSE 'Other'
            END as status_description,
            COUNT(*) as count,
            SUM(r.total_savings) as total_savings,
            AVG(r.payback) as avg_payback
        FROM recommendations r
        JOIN assessments a ON r.assessment_id = a.id
        GROUP BY a.fiscal_year, r.imp_status;
        
        -- View for ARC category summary
        CREATE VIEW IF NOT EXISTS arc_category_summary AS
        SELECT
            a.fiscal_year,
            ah.description as category,
            ah.level,
            COUNT(*) as rec_count,
            SUM(CASE WHEN r.imp_status = 'I' THEN 1 ELSE 0 END) as implemented_count,
            SUM(r.total_savings) as total_savings,
            AVG(r.payback) as avg_payback
        FROM recommendations r
        JOIN assessments a ON r.assessment_id = a.id
        JOIN arc_hierarchy ah ON r.arc = ah.arc
        GROUP BY a.fiscal_year, ah.description, ah.level;
        
        -- View for best practices tool usage
        CREATE VIEW IF NOT EXISTS best_practice_tools AS
        SELECT
            a.fiscal_year,
            r.bp_tool,
            COUNT(*) as count,
            SUM(r.total_savings) as total_savings,
            AVG(r.payback) as avg_payback
        FROM recommendations r
        JOIN assessments a ON r.assessment_id = a.id
        WHERE r.bp_tool IS NOT NULL AND r.bp_tool != ''
        GROUP BY a.fiscal_year, r.bp_tool;
        ''')
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        return {
            'status': 'success',
            'database_path': output_db_path,
            'assessments_count': len(clean_assess_df),
            'recommendations_count': len(clean_recc_df),
            'timestamp': datetime.now().isoformat()
        }