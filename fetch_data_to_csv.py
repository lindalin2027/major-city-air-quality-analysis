# this version saves all sensors' data into one CSV, accepts fuzzy date formats
from openaq import OpenAQ
import pandas as pd
import os
from dotenv import load_dotenv
from dateutil import parser 
from datetime import datetime

load_dotenv()
api_key = os.getenv("OPENAQ_API_KEY")
client = OpenAQ(api_key=api_key)

def parse_date_to_openaq_format(date_input):
    """
    Convert various date formats to OpenAQ API format (ISO 8601 with Z).
    
    Parameters:
    -----------
    date_input : str, datetime, or None
        Date in various formats:
        - "1/1/2020", "01/01/2020", "2020-1-1"
        - "January 1, 2020", "Jan 1 2020"
        - "2020-01-01", "2020/01/01"
        - datetime object
        - None (returns None)
    
    Returns:
    --------
    str
        Date in format "YYYY-MM-DDTHH:MM:SSZ"
        
    Examples:
    ---------
    >>> parse_date_to_openaq_format("1/1/2020")
    '2020-01-01T00:00:00Z'
    
    >>> parse_date_to_openaq_format("January 15, 2023")
    '2023-01-15T00:00:00Z'
    
    >>> parse_date_to_openaq_format("2024-12-31")
    '2024-12-31T00:00:00Z'
    """
    if date_input is None:
        return None
    
    # If already a datetime object
    if isinstance(date_input, datetime):
        return date_input.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # If already in correct format, return as-is
    if isinstance(date_input, str) and date_input.endswith('Z'):
        return date_input
    
    try:
        # Use dateutil.parser to handle various formats
        # dayfirst=False means 1/2/2020 = Jan 2, not Feb 1 (US format)
        dt = parser.parse(date_input, dayfirst=False)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    except Exception as e:
        raise ValueError(f"Could not parse date '{date_input}'. Error: {e}")

def get_sensor_data(client, sensor_id, datetime_from="2020-01-01T00:00:00Z", datetime_to="2025-01-01T00:00:00Z"):
    """
    Fetch data for a single sensor.
    """
    datetime_from = parse_date_to_openaq_format(datetime_from)
    datetime_to = parse_date_to_openaq_format(datetime_to)

    all_data = []
    page = 1
    
    print(f"\nFetching data for sensor {sensor_id}...")
    
    while True:
        try:
            response = client.measurements.list(
                sensors_id=sensor_id,
                datetime_from=datetime_from,  
                datetime_to=datetime_to,      
                data="days",
                limit=1000,
                page=page
            )
            
            if not response.results:
                break
            
            for result in response.results:
                all_data.append({
                    'sensor_id': sensor_id,
                    'parameter': result.parameter.name,
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
            
            if len(response.results) < 1000:
                break
            
            page += 1
            
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break
    
    df = pd.DataFrame(all_data)
    print(f"Collected {len(df)} records for sensor {sensor_id}")
    
    return df


if __name__ == '__main__':
    # Your list of sensor IDs
    sensor_ids = [1671, 1404, 564, 8330, 2183]
    
    os.makedirs('sensor_data', exist_ok=True)
    
    successful = 0
    failed = 0
    
    for idx, sensor_id in enumerate(sensor_ids, 1):
        print(f"\n[{idx}/{len(sensor_ids)}] Processing sensor {sensor_id}")
        print("=" * 70)
        
        try:
            df = get_sensor_data(client, sensor_id)
            
            if not df.empty:
                output_file = f"sensor_data/sensor_{sensor_id}_data.csv"
                df.to_csv(output_file, index=False)
                print(f"Saved: {output_file} ({len(df)} records)")
                successful += 1
            else:
                print(f"No data for sensor {sensor_id}")
                failed += 1
                
        except Exception as e:
            print(f"Error processing sensor {sensor_id}: {e}")
            failed += 1
    
    client.close()
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total: {len(sensor_ids)}")