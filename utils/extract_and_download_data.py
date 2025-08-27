import urllib.request
import zipfile
import shutil
import os
import pandas as pd
# Function to download file
def download_figshare_file(code, filename, private_link='', force_download=False):
    if len(private_link) > 0:
        link = f'https://figshare.com/ndownloader/files/{code}?private_link={private_link}'
    else:
        link = f'https://figshare.com/ndownloader/files/{code}'
    
    if (not os.path.exists(filename)) or force_download:
        print(f"Downloading {filename}")
        try:
            urllib.request.urlretrieve(link, filename)
            if os.path.exists(filename):
                print(f"Successfully downloaded {filename}.")
            else:
                print(f"Error: {filename} not found after download attempt.")
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
    else:
        print(f"{filename} already exists.")

def download_files(path_to_repo, files_to_download, private_link=None, force_download=False):
    
    df = pd.read_csv(os.path.join(path_to_repo,"file_code_mapping.csv"))
    # Create a download folder
    os.makedirs("downloads", exist_ok=True)

    # Download files
    for _, row in df.iterrows():
        filename = row['File Name']
        if filename not in files_to_download:
            continue
        code = str(row['Code']).strip()
        if code != "" and code.lower() != "nan":
            full_path = os.path.join("downloads", filename)
            download_figshare_file(code, full_path, private_link=private_link if private_link is not None else '')

# Function to unzip file
def unzip(zip_path, extract_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
