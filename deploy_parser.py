from extract_web_database import extract_web_database
from iac_database_parser import IACDatabaseParser
from arc_parser import ARCParser

def deploy_parser(UPDATE_DB=False, IAC=False, ARC=False):
    if UPDATE_DB:
        if not extract_web_database():
            print("Failed to download and extract the ITAC database.")
            return
    
    if IAC:
        iac_parser = IACDatabaseParser()

        iac_parser.load_database()
        iac_parser.load_uconn_recommendations()
        iac_parser.load_uconn_assessments()
        
        iac_parser.convert_to_database()

        iac_parser.uconn_naics_mapping.export_to_json()
    
    if ARC:
        parser = ARCParser()
        parser.generate_arc_dataframe()
        parser.generate_arc_hierarchy_tree()
        parser.upload_arc_data()

if __name__ == "__main__":
    deploy_parser(UPDATE_DB=True, IAC=True, ARC=True)