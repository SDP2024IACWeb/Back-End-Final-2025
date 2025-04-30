import pandas as pd

def parse_xls(path):
    # Visit and familiarize yourself with https://iac.university/ to know the mission and activities of IACs 
    # Visit and familiarize yourself with https://iac.university/center/UC to know the work done by the UConn IAC
    # Go to https://iac.university/#database to familiarize yourself with the IAC Database. 
    # Provided by https://iac.university/download/
    # Go to https://iac.university/technical-documents and download the database manual, The ARC: Assessment Recommendation Code System. 

    # Dictionary that contains all sheets attributed to xls file
    sheet_handler_object = {}
   
    try:
        # https://pandas.pydata.org/docs/reference/api/pandas.ExcelFile.html
        # requires openpyxl
        with pd.ExcelFile(path) as iac_df:
            for i in iac_df.sheet_names:
                sheet_handler_object.update({i: iac_df.parse(i)})
            
    except FileNotFoundError:
        print("File not found") # Check the the pathing of your IAC Database file
        return None
    
    except Exception as e:
        print("An error occurred: ", e)
        return None
    
    return sheet_handler_object