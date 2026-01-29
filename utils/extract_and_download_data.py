import urllib.request
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

# Function to download file using urllib (restored original method) with MD5 verification
def download_figshare_file(code, filename, expected_md5=None, private_link='', force_download=False):
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
                print(f"MD5 mismatch for {filename} (Expected {expected_md5}, got {local_md5}). Deleting...")
                os.remove(filename)
        else:
            print(f"{filename} already exists (No MD5 provided).")
            return

    # 2. Download loop (Retry logic)
    max_retries = 10
    attempt = 0
    
    while attempt < max_retries:
        attempt += 1
        print(f"Downloading {filename} (Attempt {attempt}/{max_retries})...")
        
        try:
            # Use urllib.request.urlretrieve as it was in the original working script
            urllib.request.urlretrieve(link, filename)
            
            if os.path.exists(filename):
                # 3. Verify Download
                if expected_md5:
                    current_md5 = calculate_md5(filename)
                    if current_md5 == expected_md5:
                        print(f"Successfully downloaded and verified: {filename}")
                        return
                    else:
                        print(f"MD5 mismatch after download! Expected {expected_md5}, got {current_md5}.")
                        print("Deleting corrupt file and retrying...")
                        os.remove(filename)
                else:
                    print(f"Successfully downloaded: {filename}")
                    return
            else:
                print(f"Error: {filename} not found after download attempt.")

        except Exception as e:
            print(f"Failed to download {filename}: {e}")
            if os.path.exists(filename):
                os.remove(filename)
            time.sleep(1) # Short pause before retry

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
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")
