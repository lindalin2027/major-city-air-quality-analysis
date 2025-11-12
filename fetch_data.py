import requests
import json
import pandas as pd
import numpy as np
import os
from datetime import datetime
from openaq import OpenAQ
from dotenv import load_dotenv  

# Load environment variables from .env file
load_dotenv() 

api_key = os.getenv("OPENAQ_API_KEY")

# Initialize client
client = OpenAQ(api_key=api_key)


def get_location_data(client, location_id, datetime_from="2020-01-01T00:00:00Z", datetime_to="2025-01-01T00:00:00Z"):
    """
    Fetch all sensor data for a specific location.
    
    Parameters:
    -----------
    client : OpenAQ
        An initialized OpenAQ client
    location_id : int
        The location ID (e.g., 1884)
    datetime_from : str
        Start date in ISO format (default: "2020-01-01T00:00:00Z")
    date_to : str
        End date in ISO format (default: "2025-01-01T00:00:00Z")
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with all daily measurements from all sensors at this location
    """
    all_data = []
    
    # Get location and sensors
    print(f"Fetching location {location_id}...")
    location_response = client.locations.get(location_id)
    sensors = location_response.results[0].sensors
    
    print(f"Found {len(sensors)} sensors")
    
    # Loop through each sensor
    for sensor in sensors:
        sensor_id = sensor.id
        parameter_name = sensor.parameter.name
        
        print(f"\nFetching data for sensor {sensor_id} ({parameter_name})...")
        
        page = 1
        
        while True:
            try:
                # Fetch daily measurements with date range
                response = client.measurements.list(
                    sensors_id=sensor_id,
                    datetime_from=datetime_from,
                    datetime_to=datetime_to,
                    data="days",  # Daily averages
                    limit=1000,
                    page=page
                )
                
                if not response.results:
                    break
                
                # Convert to DataFrame-friendly format
                for result in response.results:
                    all_data.append({
                        'sensor_id': sensor_id,
                        'parameter': parameter_name,
                        'datetime_utc': result.period.datetime_from.utc,
                        'datetime_local': result.period.datetime_from.local,
                        'value': result.value,
                        'units': result.parameter.units,
                        'coverage_percent': result.coverage.percent_complete if result.coverage else None,
                        'min': result.summary.min if result.summary else None,
                        'max': result.summary.max if result.summary else None,
                        'median': result.summary.median if result.summary else None,
                    })
                
                print(f"  Page {page}: {len(response.results)} records")
                
                # Check if there are more pages
                if len(response.results) < 1000:
                    break
                
                page += 1
                
            except Exception as e:
                print(f"  Error on page {page}: {e}")
                break
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    print(f"\n✓ Total records collected: {len(df)}")
    
    return df


if __name__ == '__main__':
    print("=" * 70)
    print("OpenAQ Data Fetcher")
    print("=" * 70)
    
    # Example 1: Get data for location 1884
    print("\nExample 1: Fetching data for location 1884 (2020-2025)")
    df_1884 = get_location_data(client, 1884)
    
    # Display summary
    print(df_1884.head())
    
    # Save to CSV
    output_file = "location_1884_data.csv"
    df_1884.to_csv(output_file, index=False)
    print(f"\nData saved to: {output_file}")