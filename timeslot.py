import csv
from datetime import datetime, timedelta

# Initialize a nested dictionary to store hours for each combination of Store, Department, Job, and Date
head_counts = {}

# Function to convert time to 15-minute intervals
def round_time_to_15_minutes(time):
    return (datetime.combine(datetime.today(), time) + timedelta(minutes=15)).time()

# Read data from CSV file
with open('C:/Users/Lenovo/Desktop/headount/S0003.csv', 'r') as csv_file:
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

# Query specific entry
query_key = ('S0003', 'MANAGEMENT', 'CTM', datetime.strptime('2024-01-14', '%Y-%m-%d').date())

# Print the result for the query
if query_key in head_counts:
    headcount_for_query = head_counts[query_key]
    print(f"Headcount for {query_key}: {headcount_for_query}")
else:
    print(f"No data found for {query_key}")
