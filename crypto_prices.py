import requests
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_crypto_prices():
    url = 'https://api.binance.com/api/v3/ticker/price'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        usd_pairs = [item for item in data if item['symbol'].endswith('USDT')]
        return usd_pairs
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Binance: {e}")
        return None

def store_in_database(df):
    try:
        # Get credentials from environment variables
        db_user = os.getenv("DB_USER")
        db_passwd = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        
        engine = create_engine(
            f"postgresql+psycopg2://{db_user}:{db_passwd}@{db_host}:{db_port}/{db_name}?sslmode=require"
        )
        
        # Send to database
        df.to_sql("crypto_coin_prices", engine, if_exists="append", index=False)
        print('Data successfully stored in PostgreSQL')
    except Exception as e:
        print(f"Error storing data in database: {e}")

def main():
    # Get crypto prices
    crypto_data = get_crypto_prices()
    
    if crypto_data:
        # Display first 30 pairs
        for item in crypto_data[:30]:
            print(f'{item["symbol"]}: {item["price"]}')
        
        # Prepare DataFrame
        df = pd.DataFrame(crypto_data)
        df['price'] = df['price'].astype(float)
        df['last_updated'] = datetime.utcnow()
        df = df.head(100)  # Limit to top 100
        
        # Store in database
        store_in_database(df)

if __name__ == "__main__":
    main()