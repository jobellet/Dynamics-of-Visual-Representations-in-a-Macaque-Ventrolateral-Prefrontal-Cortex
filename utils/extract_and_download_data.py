import urllib.request
import urllib.error
import zipfile
import shutil
import os
import pandas as pd
import hashlib
import time

# Function to calculate MD5 checksum
def calculate_md5(filepath):
    """Calculates the MD5 hexdigest of a local file."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None

# Function to download file with User-Agent and MD5 check
def download_figshare_file(code, filename, expected_md5=None, private_link='', force_download=False):
    # Construct URL
    if len(private_link) > 0:
        link = f'https://figshare.com/ndownloader/files/{code}?private_link={private_link}'
    else:
        link = f'https://figshare.com/ndownloader/files/{code}'
    
    # 1. Check if file already exists
    if os.path.exists(filename) and not force_download:
        if expected_md5:
            print(f"Verifying existing file: {filename}...")
            local_md5 = calculate_md5(filename)
            if local_md5 == expected_md5:
                print(f"Valid (MD5 verified): {filename}")
                return
            else:
                print(f"MD5 mismatch for {filename}. Expected {expected_md5}, got {local_md5}. Deleting...")
                os.remove(filename)
        else:
            print(f"{filename} already exists (No MD5 provided).")
            return

    # 2. Download loop
    max_retries = 10
    attempt = 0
    
    # Define a browser-like User-Agent to prevent 403/Blocking
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    while attempt < max_retries:
        attempt += 1
        print(f"Downloading {filename} (Attempt {attempt}/{max_retries})...")
        
        try:
            # Create a Request object with headers
            req = urllib.request.Request(link, headers=headers)
            
            # Open URL and stream to file
            with urllib.request.urlopen(req) as response, open(filename, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            
            if os.path.exists(filename):
                # 3. Verify Download
                if expected_md5:
                    current_md5 = calculate_md5(filename)
                    if current_md5 == expected_md5:
                        print(f"Successfully downloaded and verified: {filename}")
                        return
                    else:
                        print(f"MD5 mismatch! Expected {expected_md5}, got {current_md5}.")
                        # Check for the specific 'empty file' MD5
                        if current_md5 == "d41d8cd98f00b204e9800998ecf8427e":
                            print("Error: The downloaded file is empty (0 bytes). The server might be blocking the request.")
                        print("Deleting corrupt file and retrying...")
                        os.remove(filename)
                else:
                    # Basic size check if no MD5
                    if os.path.getsize(filename) > 0:
                        print(f"Successfully downloaded: {filename}")
                        return
                    else:
                        print("Error: Downloaded file is empty.")
                        os.remove(filename)
            else:
                print(f"Error: {filename} not found after download attempt.")

        except urllib.error.HTTPError as e:
            print(f"HTTP Error {e.code}: {e.reason}")
            if os.path.exists(filename):
                os.remove(filename)
            time.sleep(1)
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            if os.path.exists(filename):
                os.remove(filename)
            time.sleep(1)

    print(f"FAILED to download {filename} correctly after {max_retries} attempts.")

def download_files(path_to_repo, files_to_download, private_link=None, force_download=False):
    
    mapping_path = os.path.join(path_to_repo, "file_code_mapping.csv")
    if not os.path.exists(mapping_path):
        print(f"Error: {mapping_path} not found.")
        return

    df = pd.read_csv(mapping_path)
    # Create a download folder
    os.makedirs("downloads", exist_ok=True)

    # Download files
    for _, row in df.iterrows():
        filename = row['File Name']
        if filename not in files_to_download:
            continue
        
        code = str(row['Code']).strip()
        
        # Get MD5 if column exists, handle NaN/empty safely
        md5 = None
        if 'MD5' in row and pd.notna(row['MD5']):
            val = str(row['MD5']).strip()
            if val and val.lower() != 'nan':
                md5 = val
        
        if code != "" and code.lower() != "nan":
            full_path = os.path.join("downloads", filename)
            download_figshare_file(
                code, 
                full_path, 
                expected_md5=md5,
                private_link=private_link if private_link is not None else '',
                force_download=force_download
            )

# Function to unzip file
def unzip(zip_path, extract_path):
    print(f"Unzipping {zip_path}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        print("Unzip successful.")
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file or is empty.")
    except FileNotFoundError:
        print(f"Error: {zip_path} not found.")
