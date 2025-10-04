import pandas as pd
import os
import time
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

LIMIT = 1000

def run_stock_fetch():
    print(f"Job started at {datetime.now()}")
    fetch_and_save_tickers()
    print(f"Job finished at {datetime.now()}")

def fetch_and_save_tickers():
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&locale=in&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={os.getenv('POLYGON_API_KEY')}"

    print("Fetching data from Polygon API...")

    response = requests.get(url)

    tickers = []
    data = response.json()

    if "results" not in data:
        raise Exception("No results found in the response.")

    for ticker in data['results']:
        tickers.append(ticker)

    while 'next_url' in data and data['next_url']:
        time.sleep(2)
        response = requests.get(data['next_url'] + f"&apiKey={os.getenv('POLYGON_API_KEY')}")
        
        data = response.json()
        if "results" not in data:
            print(data)
            break
        for ticker in data['results']:
            tickers.append(ticker)
    df = pd.DataFrame(tickers)
    df.to_csv('tickers.csv', index=False)
    print(f"Saved {len(tickers)} tickers to tickers.csv")
    
if __name__ == "__main__":
    run_stock_fetch()