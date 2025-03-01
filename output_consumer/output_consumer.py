import os
import json
import csv
from kafka import KafkaConsumer

# Get environment variables
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'output_topic')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
SLIDE_DURATION_STR = os.getenv('SLIDE_DURATION', '10 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()
NUM_CORES = os.getenv('SPARK_NUM_CORES', '20')
NUM_THREADS = os.getenv('NUM_THREADS', '1')
EXPERIMENT_TYPE = os.getenv('EXPERIMENT_TYPE', 'kafka')
MEMORY_LIMIT = os.getenv('MEMORY_LIMIT', '8G')


# Kafka keys for different message types
STREAM_DAQ_KEY = os.getenv('STREAM_DAQ_KEY', 'stream-daq')
DEEQU_KEY = os.getenv('DEEQU_KEY', 'deequ')
BLAST_KEY: str = os.getenv('BLAST_KEY', 'blast')

def get_window_short_name(window_type: str, window_duration: str, slide_duration: str | None,
                         gap_duration: str | None) -> str:
    window_type = str(window_type).lower()
    match window_type:
        case "tumbling":
            return f"{window_type}_{window_duration}"
        case "sliding":
            return f"{window_type}_{window_duration}_{slide_duration}"
        case "session":
            return f"{window_type}_{window_duration}_{gap_duration}"

# This CSV will store final aggregated lines when blasts complete
FINAL_RESULTS_CSV = "data/final_performance.csv"

print(f"Starting output consumer. Listening to topic: {OUTPUT_TOPIC}")
print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Final aggregated CSV path: {FINAL_RESULTS_CSV}")

def initialize_or_reset_performance_state() -> dict:
    """
    Returns a dictionary holding performance metrics for each tool (stream-daq, deequ).
    We track:
      - number_of_windows (int)
      - max_timestamp (int -> the largest broker timestamp we've seen for that tool)
    """
    return {
        STREAM_DAQ_KEY: {
            'number_of_windows': 0,
            'max_timestamp': -1,
            'processing_time': 0
        },
        DEEQU_KEY: {
            'number_of_windows': 0,
            'max_timestamp': -1,
            'processing_time': 0
        }
    }

def update_performance_metrics_state(performance_state: dict, new_result: dict, tool_key: str) -> None:
    """
    Updates the in-memory state (performance_state) based on a new message from 'stream-daq' or 'deequ'.
    We do two things:
      1) Increment number_of_windows by one.
      2) Update max_timestamp if the new message's 'timestamp' is greater than the current stored one.
    """
    print(f"Just received new message: {new_result} for tool: {tool_key}")
    print()
    if tool_key not in performance_state:
        # Unknown tool -> ignore
        print(f"Warning: Received unknown tool key = {tool_key}, ignoring.")
        return

    performance_state[tool_key]['number_of_windows'] += 1
    new_timestamp = new_result.get('timestamp', -1)
    if new_timestamp > performance_state[tool_key]['max_timestamp']:
        performance_state[tool_key]['max_timestamp'] = new_timestamp

    new_processing_time = new_result.get('checkingTimeMs', new_result['timestamp'] - new_result['time'])
    performance_state[tool_key]['processing_time'] += new_processing_time

def write_results_to_file(state: dict, blast_info: dict) -> None:
    """
    Takes the current in-memory state dict, plus the blast_info dictionary containing:
      - blast_id
      - blast_start_time
      - blast_end_time
      - blast_size
    Writes each tool's performance metrics as a single row in a CSV with columns:
      tool, blast_id, blast_size, blast_start_time, blast_end_time, number_of_windows, max_timestamp
    """
    # We define the final CSV columns:
    final_fieldnames = [
        "tool", "blast_id", "blast_size", "blast_start_time",
        "blast_end_time", "number_of_windows", "max_timestamp",
        "processing_time", "window_setting", "number_of_cores", "number_of_threads",
        "memory_limit", "experiment_type"
    ]

    blast_id = blast_info.get("blast_id", -1)
    blast_size = blast_info.get("blast_size", -1)
    blast_start = blast_info.get("blast_start_time", -1)
    blast_end = blast_info.get("blast_end_time", -1)
    window_setting = get_window_short_name(WINDOW_TYPE, WINDOW_DURATION_STR, SLIDE_DURATION_STR, GAP_DURATION_STR)
    number_of_cores = NUM_CORES
    number_of_threads = NUM_THREADS

    # Open the final CSV in append mode
    file_exists = os.path.isfile(FINAL_RESULTS_CSV)
    with open(FINAL_RESULTS_CSV, 'a', newline='') as results_file:
        writer = csv.DictWriter(results_file, fieldnames=final_fieldnames)
        if not file_exists:
            writer.writeheader()

        # Write one line for stream-daq if the state indicates Stream DaQ is running
        daq_data = state[STREAM_DAQ_KEY]
        if daq_data["max_timestamp"] > 0:
            writer.writerow({
                "tool": STREAM_DAQ_KEY,
                "blast_id": blast_id,
                "blast_size": blast_size,
                "blast_start_time": blast_start,
                "blast_end_time": blast_end,
                "number_of_windows": daq_data['number_of_windows'],
                "max_timestamp": daq_data['max_timestamp'],
                "processing_time": daq_data['processing_time'],
                "window_setting": window_setting,
                "number_of_cores": number_of_cores,
                "number_of_threads": number_of_threads,
                "memory_limit": MEMORY_LIMIT,
                "experiment_type": EXPERIMENT_TYPE
            })

        # One line for deequ if the state indicates Deequ is running
        deequ_data = state[DEEQU_KEY]
        if deequ_data["max_timestamp"] > 0:
            writer.writerow({
                "tool": DEEQU_KEY,
                "blast_id": blast_id,
                "blast_size": blast_size,
                "blast_start_time": blast_start,
                "blast_end_time": blast_end,
                "number_of_windows": deequ_data['number_of_windows'],
                "max_timestamp": deequ_data['max_timestamp'],
                "processing_time": deequ_data['processing_time'],
                "window_setting": window_setting,
                "number_of_cores": number_of_cores,
                "number_of_threads": number_of_threads,
                "memory_limit": MEMORY_LIMIT,
                "experiment_type": EXPERIMENT_TYPE
            })

    print(f"Appended final performance lines for blast_id={blast_id} to {FINAL_RESULTS_CSV}")

def main():
    # Create Kafka consumer
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Initialize in-memory performance state
    state = initialize_or_reset_performance_state()

    try:
        for message in consumer:
            message_value = message.value  # Dictionary representing the message

            try:
                message_key = message.key.decode('utf-8') if message.key else "unknown"
            except Exception as e:
                message_key = "unknown"

            # Add the broker timestamp field to the dict
            message_value['timestamp'] = message.timestamp # when kafka broker received the message; in ms

            # Classify message based on key
            if message_key == BLAST_KEY:
                # We finalize the metrics for the previous blast
                # by writing them to final CSV, then reset state
                write_results_to_file(state, message_value)
                state = initialize_or_reset_performance_state()
            elif message_key == STREAM_DAQ_KEY or message_key == DEEQU_KEY:
                # Update the in-memory metrics for the respective tool
                update_performance_metrics_state(state, message_value, message_key)
            else:
                update_performance_metrics_state(state, message_value, STREAM_DAQ_KEY)

    except KeyboardInterrupt:
        print("Stopping output consumer…")
    finally:
        print("Reached the finally block. Closing consumer...")
        consumer.close()

if __name__ == "__main__":
    main()