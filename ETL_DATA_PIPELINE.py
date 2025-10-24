import os
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# === CONFIGURATION ===
DATA_FOLDER = "data/"       # Folder containing daily CSV files
DB_NAME = "compost_data.db" # SQLite database file
TABLE_NAME = "sensor_readings"
READING_INTERVAL_MIN = 10   # Assumed time gap between sensor readings (in minutes)

# === EXTRACT ===
def extract_data():
    all_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]
    df_list = []

    for f in all_files:
        file_path = os.path.join(DATA_FOLDER, f)
        df = pd.read_csv(file_path)

        # Extract date from filename (e.g., compost_2025-11-13.csv)
        try:
            date_part = f.split('_')[-1].split('.')[0]
            base_date = datetime.strptime(date_part, "%Y-%m-%d")
        except Exception:
            print(f"Warning: Could not extract date from {f}. Using current date instead.")
            base_date = datetime.now()

        # Generate synthetic timestamps (10-minute intervals)
        df['timestamp'] = [base_date + timedelta(minutes=i * READING_INTERVAL_MIN) for i in range(len(df))]
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    print(f"Extracted {len(combined_df)} total rows from {len(all_files)} files.")
    return combined_df

# === TRANSFORM ===
def transform_data(df):
    df = df.drop_duplicates()
    df = df.interpolate(method='linear', limit_direction='forward')

    # Remove invalid readings
    if 'temperature' in df.columns:
        df = df[(df['temperature'] >= 0) & (df['temperature'] <= 100)]
    if 'humidity' in df.columns:
        df = df[(df['humidity'] >= 0) & (df['humidity'] <= 100)]

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print("Data cleaned and transformed successfully.")
    return df

# === LOAD ===
def load_data(df):
    conn = sqlite3.connect(DB_NAME)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Loaded {len(df)} records into '{DB_NAME}' (table: '{TABLE_NAME}').")

# === VISUALIZE ===
def visualize_data(df):
    plt.figure(figsize=(10, 5))
    if 'temperature' in df.columns:
        plt.plot(df['timestamp'], df['temperature'], label='Temperature (°C)')
    if 'humidity' in df.columns:
        plt.plot(df['timestamp'], df['humidity'], label='Humidity (%)')
    if 'NH3' in df.columns:
        plt.plot(df['timestamp'], df['NH3'], label='NH3 (ppm)')
    plt.xlabel('Timestamp')
    plt.ylabel('Value')
    plt.title('Compost Sensor Data Trends')
    plt.legend()
    plt.tight_layout()
    plt.show()

# === MAIN FLOW ===
if __name__ == "__main__":
    raw_df = extract_data()
    clean_df = transform_data(raw_df)
    load_data(clean_df)
    visualize_data(clean_df)
