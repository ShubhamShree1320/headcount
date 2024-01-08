import pandas as pd
import os
import csv
from datetime import datetime, timedelta
import json
output_dir="C:/Users/Lenovo/Desktop/headount/"
def read_input_data(input_csv_path):
    df = pd.read_csv(input_csv_path)
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%b-%y')
    return df

def read_store_lookup_data(lookup_csv_path):
    return pd.read_csv(lookup_csv_path)

def round_time_to_15_minutes(time):
    return (datetime.combine(datetime.today(), time) + timedelta(minutes=15)).time()

def process_head_counts(csv_path):
    head_counts = {}

    # Function to convert time to 15-minute intervals
    def round_time_to_15_minutes(time):
        return (datetime.combine(datetime.today(), time) + timedelta(minutes=15)).time()

    # Read data from CSV file
    with open(csv_path) as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            store = row[0]
            department = row[1]
            job = row[2]
            date = datetime.strptime(row[3], '%Y-%m-%d').date()
            time_slot = datetime.strptime(row[4], '%H:%M').time()
            hours = int(row[5])

            # Round time to 15-minute intervals
            rounded_time_slot = round_time_to_15_minutes(time_slot)

            # Update the nested dictionary with the hours for each combination
            key = (store, department, job, date)
            if key not in head_counts:
                head_counts[key] = [0] * 96  # Initialize with 96 zeros

            # Calculate the index for the time slot
            index = (rounded_time_slot.hour * 4) + (rounded_time_slot.minute // 15)

            # Update the value in the list
            head_counts[key][index] += hours

    return head_counts

def generate_json_structure(store_data, lookup_data, time_slot_hours):
    json_data = []
    # Convert 'Date' column to datetime
    store_data['Date'] = pd.to_datetime(store_data['Date'])
    
    for index, row in store_data.iterrows():
        if pd.notna(row['Date']):  # Check for NaT before formatting
            query_key = (row['STORE'], row['Department'], row['Job'], row['Date'].date())
            json_structure = {
                "Start_Date": row['Date'].strftime('%Y-%m-%d'),
                "sitePath": row['STORE_PATH'],
                "orgJobPath": f"{row['STORE_PATH']}/{row['Department']}/{row['Job']}",
                "headCount": time_slot_hours[query_key]
            }
            json_data.append(json_structure)
                        # Save JSON structure to a separate file
            file_name = f"{row['STORE']}_{row['Department']}_{row['Job']}_{row['Date'].strftime('%Y-%m-%d')}.json"
            file_path = os.path.join(output_dir, file_name)

            with open(file_path, 'w') as json_file:
                json.dump(json_structure, json_file, indent=0)

    return json_data
def main():
    input_csv_path = r"C:/Users/Lenovo/Desktop/shubh.csv"
    lookup_csv_path = r"C:/Users/Lenovo/Desktop/Store_Lookup_Table.csv"

    df_input_data = read_input_data(input_csv_path)
    df_store_lookup_data = read_store_lookup_data(lookup_csv_path)

    csv_paths = []
    for store, group in df_input_data.groupby('Store'):
        csv_name = f"{store}.csv"
        csv_paths.append(os.path.abspath(csv_name))
        group.to_csv(csv_name, index=False)

    json_data = []
    for csv_path in csv_paths:
        df_store_data = pd.read_csv(csv_path)
        merged_data = pd.merge(df_store_lookup_data, df_store_data, how='left', left_on='STORE', right_on='Store')
        time_slot_hours = process_head_counts(csv_path)
        json_data.extend(generate_json_structure(merged_data, df_store_lookup_data, time_slot_hours))

    final_json_str = json.dumps(json_data)
    print(final_json_str)

if __name__ == "__main__":
    main()
