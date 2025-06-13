import os
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from pathlib import Path

# Example country map
countries = {
    "CRI": "Costa Rica",
    "CUB": "Cuba"
}

# Settings
base_url_template = "https://data.worldpop.org/GIS/AgeSex_structures/Global_2000_2020/{year}/{code}/"
output_base_path = Path("D:/Data/Population_Elderly")
ages = [60, 65, 70, 75, 80]
genders = ["f", "m"]
years = [2015, 2020]
max_threads = 10

# Build tasks
tasks = []
for code, name in countries.items():
    for year in years:
        for age in ages:
            for gender in genders:
                filename = f"{code.lower()}_{gender}_{age}_{year}.tif"
                url = base_url_template.format(year=year, code=code) + filename
                out_dir = output_base_path / name
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / filename
                tasks.append((url, out_path))

# Download function with single progress bar
def download_file(url, out_path):
    try:
        # HEAD request to get the source file size
        head = requests.head(url, timeout=10)
        head.raise_for_status()
        total = int(head.headers.get('content-length', 0))

        # Skip if file exists and matches source size
        if out_path.exists():
            local_size = out_path.stat().st_size
            if local_size == total:
                return  # File already exists and is complete
            else:
                print(f"Redownloading incomplete file: {out_path.name}")

        # Proceed to download
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(out_path, 'wb') as f, tqdm(
                total=total,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=out_path.name,
                leave=True
            ) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))

    except Exception as e:
        print(f"Error downloading {out_path.name}: {e}")

# Download all using threads
def download_all(tasks):
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(download_file, url, path) for url, path in tasks]
        for future in futures:
            future.result()

# Run it
if __name__ == "__main__":
    download_all(tasks)
