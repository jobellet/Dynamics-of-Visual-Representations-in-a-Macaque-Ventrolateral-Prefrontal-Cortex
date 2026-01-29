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

def download_figshare_file(code, filename, expected_md5=None, private_link='', token=None, force_download=False):
    # Construct URL
    if private_link:
        url = f'https://figshare.com/ndownloader/files/{code}?private_link={private_link}'
    else:
        url = f'https://figshare.com/ndownloader/files/{code}'
    
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
    
    while attempt < max_retries:
        attempt += 1
        print(f"Downloading {filename} (Attempt {attempt}/{max_retries})...")
        
        try:
            # Create Request object to allow headers
            req = urllib.request.Request(url)
            
            # [CRITICAL] User-Agent mimics a browser. 
            # This ensures the script works for PUBLIC files in the future without a token.
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
            
            # [CRITICAL] Authorization header for PRIVATE/EMBARGOED files.
            # You only need to pass 'token' while the dataset is private.
            if token:
                req.add_header('Authorization', f'token {token}')

            # Stream download
            with urllib.request.urlopen(req) as response, open(filename, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            
            # 3. Verify Download
            if os.path.exists(filename):
                if expected_md5:
                    current_md5 = calculate_md5(filename)
                    if current_md5 == expected_md5:
                        print(f"Successfully downloaded and verified: {filename}")
                        return
                    else:
                        print(f"MD5 mismatch after download! Expected {expected_md5}, got {current_md5}.")
                        # Check for 'empty file' hash
                        if current_md5 == "d41d8cd98f00b204e9800998ecf8427e":
                            print("Error: File is empty (server likely blocked request).")
                        
                        print("Deleting corrupt file...")
                        os.remove(filename)
                else:
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
            # If 403 Forbidden, it usually means the Token is missing/wrong OR the User-Agent is blocked
            if e.code == 403 and not token:
                print("Hint: For private files, ensure your Token is correct.")
            time.sleep(1)
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            time.sleep(1)

    print(f"FAILED to download {filename} correctly after {max_retries} attempts.")

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

# Function to unzip file
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
