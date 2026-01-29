import os
import shutil
import zipfile
import hashlib
import time
import requests
import pandas as pd
from requests.exceptions import RequestException

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

def download_figshare_file(code, filename, expected_md5=None, private_link='', token=None, force_download=False):
    """
    Downloads a file from Figshare using requests.
    Handles redirects and authentication to bypass AWS WAF blocking.
    """
    # Construct URL with private link if present
    url = f'https://figshare.com/ndownloader/files/{code}'
    params = {}
    if private_link:
        params['private_link'] = private_link

    # 1. Check if file already exists
    if os.path.exists(filename) and not force_download:
        if expected_md5:
            print(f"Verifying existing file: {filename}...")
            local_md5 = calculate_md5(filename)
            if local_md5 == expected_md5:
                print(f"Valid (MD5 verified): {filename}")
                return
            else:
                print(f"MD5 mismatch for {filename}. Deleting and re-downloading...")
                os.remove(filename)
        else:
            print(f"{filename} already exists (No MD5 provided).")
            return

    # 2. Download loop
    max_retries = 5
    attempt = 0
    
    # Headers to mimic a real browser (Critical for public access later)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    # Add Token if provided (Critical for private access now)
    if token:
        headers['Authorization'] = f'token {token}'

    while attempt < max_retries:
        attempt += 1
        print(f"Downloading {filename} (Attempt {attempt}/{max_retries})...")
        
        try:
            # Use requests.get with stream=True
            with requests.get(url, params=params, headers=headers, stream=True, allow_redirects=True) as r:
                r.raise_for_status() # Raises error for 403/404
                
                # Check for empty content before writing
                if r.headers.get('Content-Length') == '0':
                    raise ValueError("Server returned 0 bytes.")

                with open(filename, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)

            # 3. Verify Download
            if os.path.exists(filename):
                if os.path.getsize(filename) == 0:
                     print("Error: File is empty (0 bytes).")
                     os.remove(filename)
                     time.sleep(1)
                     continue

                if expected_md5:
                    current_md5 = calculate_md5(filename)
                    if current_md5 == expected_md5:
                        print(f"Successfully downloaded and verified: {filename}")
                        return
                    else:
                        print(f"MD5 mismatch! Expected {expected_md5}, got {current_md5}.")
                        # Check for the specific 'empty file' hash
                        if current_md5 == "d41d8cd98f00b204e9800998ecf8427e":
                             print("Error: File is effectively empty.")
                        
                        print("Deleting corrupt file...")
                        os.remove(filename)
                else:
                    print(f"Successfully downloaded: {filename}")
                    return
            else:
                print(f"Error: {filename} not found after download.")

        except RequestException as e:
            print(f"HTTP Error: {e}")
            if "403" in str(e) and not token:
                print("Hint: 403 Forbidden. Ensure you are passing your 'token' in download_files().")
            time.sleep(1)
        except Exception as e:
            print(f"Download Error: {e}")
            if os.path.exists(filename):
                os.remove(filename)
            time.sleep(1)

    print(f"FAILED to download {filename} after {max_retries} attempts.")
    print("MANUAL DOWNLOAD REQUIRED: If this persists, download the file manually from Figshare and place it in the 'downloads' folder.")

def download_files(path_to_repo, files_to_download, private_link=None, token=None, force_download=False):
    
    mapping_path = os.path.join(path_to_repo, "file_code_mapping.csv")
    if not os.path.exists(mapping_path):
        print(f"Error: {mapping_path} not found.")
        return

    df = pd.read_csv(mapping_path)
    os.makedirs("downloads", exist_ok=True)

    for _, row in df.iterrows():
        filename = row['File Name']
        if filename not in files_to_download:
            continue
        
        code = str(row['Code']).strip()
        
        # Get MD5 if column exists
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
                token=token,
                force_download=force_download
            )

def unzip(zip_path, extract_path):
    print(f"Unzipping {zip_path}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        print("Unzip successful.")
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")
    except FileNotFoundError:
        print(f"Error: {zip_path} not found.")
