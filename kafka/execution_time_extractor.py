import os, json
import pandas as pd
from config.configurations import (
    number_of_elements_to_send, sleep_seconds_between_messages
)


def get_greatest_timestamp(file_path, has_two_values):
    if "quix" in str(file_path):
        df = pd.read_csv(file_path)
    elif "pathway" in str(file_path):
        df = pd.read_csv(file_path)
        df["timestamp"] = df["time"]
    # Read the CSV file
    elif "faust" in str(file_path):
        df = pd.read_csv(file_path, header=None, names=['value1', 'value2', 'timestamp'])
    elif "bytewax" in str(file_path):
        df = pd.read_csv(file_path, header=None, names=['value1','timestamp'])
    else:
        df = pd.read_csv(file_path, header=None, names=['value1', 'value2', 'timestamp'])

    # Return the greatest timestamp
    return df['timestamp'].astype(int).max()


def get_start_timestamp():
    with open("../framework-comparisons/results/kafka_stream_report.json", 'r') as report_file:
        return int(json.load(report_file)['start'])



def main():
    base_dir = '../framework-comparisons/results'
    timestamps = {}
    start_timestamp = get_start_timestamp()
    print("Start timestamp:", start_timestamp)

    for dir_name in os.listdir(base_dir):
        dir_path = os.path.join(base_dir, dir_name)

        if os.path.isdir(dir_path):
            csv_files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]

            if len(csv_files) == 2:
                # Two CSV files
                max_timestamps = []
                for csv_file in csv_files:
                    file_path = os.path.join(dir_path, csv_file)
                    max_timestamps.append(get_greatest_timestamp(file_path, has_two_values=True))
                greatest_timestamp = max(max_timestamps)
            elif len(csv_files) == 1:
                # One CSV file
                file_path = os.path.join(dir_path, csv_files[0])
                greatest_timestamp = get_greatest_timestamp(file_path, has_two_values=False)
            else:
                continue

            timestamps[dir_name] = greatest_timestamp - start_timestamp - (number_of_elements_to_send * sleep_seconds_between_messages)
            print(f"Execution time in {dir_name}: {timestamps[dir_name]}")
            with open(dir_path + " executions.txt", 'a') as file:
                file.write(str(timestamps[dir_name]))
                file.write("\n")

    # Plotting
    # plt.figure(figsize=(10, 6))
    # plt.bar(timestamps.keys(), timestamps.values(), color='skyblue')
    # plt.xlabel('Directory')
    # plt.ylabel('Greatest Timestamp')
    # plt.title('Greatest Timestamp by Directory')
    # plt.xticks(rotation=45)
    # plt.tight_layout()
    # plt.show()


if __name__ == "__main__":
    main()