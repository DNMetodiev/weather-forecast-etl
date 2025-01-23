import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load API key and database credentials from .env
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
DB_URL = os.getenv("POSTGRES_DB_URL")  # Format: postgresql://username:password@localhost/dbname

def fetch_forecast(city):
    """
    Fetches the 7-day weather forecast for a city using the OpenWeatherMap API.
    Args:
        city (str): Name of the city.
    Returns:
        dict: Raw JSON data with forecast information.
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
    Processes raw forecast data into a structured DataFrame.
    Args:
        raw_data (dict): Raw JSON data from the API.
    Returns:
        pandas.DataFrame: Structured forecast data.
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
    Loads forecast data into a PostgreSQL database table.
    Args:
        dataframe (pandas.DataFrame): The processed forecast data.
        table_name (str): Name of the table.
    """
    engine = create_engine(DB_URL)
    dataframe.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Data loaded into PostgreSQL table '{table_name}'")

if __name__ == "__main__":
    # List of cities to fetch forecasts for
    cities = ["London", "Paris", "New York", "Tokyo", "Mumbai"]

    # DataFrame to hold all forecasts
    all_forecasts = pd.DataFrame()

    # Fetch and process forecasts for each city
    for city in cities:
        print(f"Fetching forecast for {city}...")
        raw_data = fetch_forecast(city)

        if raw_data:
            processed_data = process_forecast(raw_data)
            all_forecasts = pd.concat([all_forecasts, processed_data], ignore_index=True)

    # Display the final DataFrame
    print("Final Processed Data:")
    print(all_forecasts)

    # Load data into PostgreSQL
    load_to_postgres(all_forecasts)
