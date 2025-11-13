from openaq import OpenAQ
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from dateutil import parser
from datetime import datetime

load_dotenv()
api_key = os.getenv("OPENAQ_API_KEY")
database_url = os.getenv("DATABASE_URL")

client = OpenAQ(api_key=api_key)


def parse_date_to_openaq_format(date_input):
    """Convert various date formats to OpenAQ API format."""
    if date_input is None:
        return None
    if isinstance(date_input, datetime):
        return date_input.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(date_input, str) and date_input.endswith('Z'):
        return date_input
    try:
        dt = parser.parse(date_input, dayfirst=False)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        raise ValueError(f"Could not parse date '{date_input}'. Error: {e}")


def get_sensor_data(client, sensor_id, datetime_from="2020-01-01", datetime_to="2025-01-01"):
    """Fetch data for a single sensor."""
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
                    'min_value': result.summary.min if result.summary else None,
                    'max_value': result.summary.max if result.summary else None,
                    'median_value': result.summary.median if result.summary else None,
                })
            
            print(f"  Page {page}: {len(response.results)} records")
            
            if len(response.results) < 1000:
                break
            
            page += 1
            
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break
    
    df = pd.DataFrame(all_data)
    print(f"✓ Collected {len(df)} records")
    return df


def save_to_postgres(df):
    """Save DataFrame to PostgreSQL."""
    if df.empty:
        print("⚠️  Empty DataFrame, nothing to save")
        return
    
    engine = create_engine(database_url)
    
    # Convert datetime columns
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])
    df['datetime_local'] = pd.to_datetime(df['datetime_local'])
    
    try:
        df.to_sql(
            'air_quality_measurements', 
            engine, 
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"✓ Saved {len(df)} records to PostgreSQL")
    except Exception as e:
        print(f"✗ Error saving to database: {e}")


if __name__ == '__main__':
    print("=" * 70)
    print("FETCHING DATA AND SAVING TO POSTGRESQL")
    print("=" * 70)
    
    # Test with a few sensors
    sensor_ids = [1884, 2178, 1102]
    
    for idx, sensor_id in enumerate(sensor_ids, 1):
        print(f"\n[{idx}/{len(sensor_ids)}] Processing sensor {sensor_id}")
        print("-" * 70)
        
        try:
            df = get_sensor_data(client, sensor_id, "1/1/2023", "12/31/2023")
            
            if not df.empty:
                save_to_postgres(df)
            
        except Exception as e:
            print(f"✗ Error: {e}")
    
    client.close()
    print("\n✓ Complete!")