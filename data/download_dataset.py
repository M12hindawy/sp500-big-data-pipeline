"""
Utility to download the Kaggle dataset safely if credentials are provided.
Dataset: darkmatternet/s-and-p-500-stocks-25-years-of-data-updated-daily
"""
import os
import sys
import shutil
from pathlib import Path

DATASET_NAME = "darkmatternet/s-and-p-500-stocks-25-years-of-data-updated-daily"
DATA_DIR = Path(__file__).resolve().parent

def download_with_kagglehub():
    try:
        import kagglehub
        print(f"Downloading {DATASET_NAME} via kagglehub...")
        path = kagglehub.dataset_download(DATASET_NAME)
        print(f"Downloaded to cache: {path}")
        # Copy files to data dir
        for file_name in ["sp500_stocks.csv", "sp500_companies.csv"]:
            src = Path(path) / file_name
            dst = DATA_DIR / file_name
            if src.exists():
                print(f"Copying {src} -> {dst}")
                shutil.copy2(src, dst)
        print("Kaggle download completed successfully.")
        return True
    except Exception as e:
        print(f"kagglehub download failed or not installed: {e}")
        return False

def download_with_kaggle_api():
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
        print(f"Downloading {DATASET_NAME} via Kaggle API...")
        api.dataset_download_files(DATASET_NAME, path=str(DATA_DIR), unzip=True)
        print("Download and unzip completed successfully.")
        return True
    except Exception as e:
        print(f"Kaggle API download failed: {e}")
        return False

if __name__ == "__main__":
    if (DATA_DIR / "sp500_stocks.csv").exists() and (DATA_DIR / "sp500_companies.csv").exists():
        print(f"Dataset files already present in {DATA_DIR}")
        sys.exit(0)

    if not download_with_kagglehub() and not download_with_kaggle_api():
        print("\nCould not automatically download the dataset.")
        print("Please follow instructions in DATASET_SETUP.md to place:")
        print(f" - {DATA_DIR / 'sp500_stocks.csv'}")
        print(f" - {DATA_DIR / 'sp500_companies.csv'}")
        sys.exit(1)
