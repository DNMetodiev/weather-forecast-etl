import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import plotly.express as px

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

def visualize_weather_data(dataframe):
    """
    Creates visualizations for the weather data.
    """
    # Line chart: Temperature trends over time for each city
    plt.figure(figsize=(12, 6))
    for city in dataframe['city'].unique():
        city_data = dataframe[dataframe['city'] == city]
        plt.plot(city_data['date'], city_data['temperature'], label=city)
    plt.title("Temperature Trends Over Time")
    plt.xlabel("Date")
    plt.ylabel("Temperature (°C)")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("temperature_trends.png")  # Save the chart as an image
    plt.show()

    # Scatter plot: Temperature vs. Humidity
    fig = px.scatter(
        dataframe,
        x="temperature",
        y="humidity",
        color="city",
        title="Temperature vs Humidity",
        labels={"temperature": "Temperature (°C)", "humidity": "Humidity (%)"},
        template="plotly"
    )
    fig.write_html("temperature_vs_humidity.html")  # Save as an interactive HTML file
    fig.show()

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

    # Save data to PostgreSQL
    load_to_postgres(all_forecasts)

    # Visualize the data
    visualize_weather_data(all_forecasts)
