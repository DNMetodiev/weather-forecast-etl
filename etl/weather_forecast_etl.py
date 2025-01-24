import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Read credentials from the .env file
API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_URL = os.getenv("POSTGRES_DB_URL")

if not API_KEY:
    raise Exception("API Key not found. Please ensure it is set in the .env file.")
if not DB_URL:
    raise Exception("Database URL not found. Please ensure it is set in the .env file.")

def fetch_forecast(city):
    """
    Fetches the 5-day weather forecast for a city.
    """
    url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching forecast for {city}: {response.status_code}, {response.text}")
        return None

def process_forecast(raw_data):
    """
    Processes raw forecast data into a DataFrame.
    """
    city = raw_data["city"]["name"]
    forecasts = []
    for entry in raw_data["list"]:
        forecasts.append({
            "city": city,
            "date": entry["dt_txt"],
            "temperature": entry["main"]["temp"],
            "humidity": entry["main"]["humidity"],
            "weather": entry["weather"][0]["description"],
            "wind_speed": entry["wind"]["speed"]
        })
    return pd.DataFrame(forecasts)

def load_to_postgres(dataframe, table_name="weather_forecasts"):
    """
    Loads the DataFrame into a PostgreSQL table.
    """
    engine = create_engine(DB_URL)
    dataframe.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Data loaded into table '{table_name}'")

if __name__ == "__main__":
    cities = ["London", "Paris", "New York", "Tokyo", "Mumbai"]
    all_forecasts = pd.DataFrame()

    for city in cities:
        print(f"Fetching forecast for {city}...")
        raw_data = fetch_forecast(city)
        if raw_data:
            processed_data = process_forecast(raw_data)
            all_forecasts = pd.concat([all_forecasts, processed_data], ignore_index=True)

    print("Final Processed Data:")
    print(all_forecasts)

    load_to_postgres(all_forecasts)
