import os
import requests
import zipfile
import shutil
from config import Config

def ensure_directory_exists(directory_path):
    """Ensure the specified directory exists, creating it if necessary."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok=True)
        print(f"Created directory: {directory_path}")

def download_database_file(url, save_path):
    """
    Downloads the ITAC database ZIP file from the provided URL.
    
    Args:
        url (str): URL to download the file from
        save_path (str): Where to save the downloaded ZIP file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Downloading database from {url}")
        response = requests.get(url, stream=True)
        if response.status_code != 200:
            print(f"Failed to download file: Status code {response.status_code}")
            return False
        
        # Save the file
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Download completed and saved to {save_path}")
        return True
    except Exception as e:
        print(f"Error downloading file: {str(e)}")
        return False

def extract_zip_file(zip_path, extract_path):
    """
    Extract the downloaded ZIP file containing the ITAC database.
    
    Args:
        zip_path (str): Path to the ZIP file
        extract_path (str): Directory to extract the contents to
        
    Returns:
        str: Path to the extracted XLS file or None if failed
    """
    try:
        print(f"Extracting ZIP file {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # Look for the XLS file in the extracted directory
        for file in os.listdir(extract_path):
            if file.endswith('.xls'):
                return os.path.join(extract_path, file)
        
        print("No XLS file found in the extracted contents")
        return None
    except Exception as e:
        print(f"Error extracting ZIP file: {str(e)}")
        return None

def extract_web_database():
    """
    Main function to download and extract the ITAC database.
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Set up the necessary paths
    zip_download_path = os.path.join(Config.EXCEL_FILE_DIR, 'ITAC_Database.zip')
    temp_extract_path = os.path.join(Config.EXCEL_FILE_DIR, 'temp_extract')
    final_database_path = Config.ITAC_DATABASE_PATH
    
    # Ensure directories exist
    ensure_directory_exists(Config.EXCEL_FILE_DIR)
    ensure_directory_exists(temp_extract_path)
    
    try:
        # Step 1: Download the database zip file
        download_url = "https://itac.university/storage/ITAC_Database.zip"
        if not download_database_file(download_url, zip_download_path):
            return False
        
        # Step 2: Extract the zip file
        xls_path = extract_zip_file(zip_download_path, temp_extract_path)
        if not xls_path:
            return False
        
        # Step 3: Move the XLS file to the final destination
        shutil.copy2(xls_path, final_database_path)
        print(f"Successfully copied database to {final_database_path}")
        
        # Clean up
        try:
            os.remove(zip_download_path)
            shutil.rmtree(temp_extract_path)
            print("Cleaned up temporary files")
        except Exception as e:
            print(f"Warning: Could not clean up temporary files: {str(e)}")
        
        return True
    except Exception as e:
        print(f"Error in extract_web_database: {str(e)}")
        return False

if __name__ == "__main__":
    print("Starting ITAC database extraction")
    
    success = extract_web_database()
    
    if success:
        print("ITAC database extraction completed successfully")
    else:
        print("ITAC database extraction failed")