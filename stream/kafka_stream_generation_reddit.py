import os
import csv
import json
import time
from kafka import KafkaProducer

# Environment variables
KAFKA_INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
KAFKA_OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'data_output')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDDIT_CSV_FILE = os.getenv('REDDIT_CSV_FILE', 'datasets/reddit-comments-may-2015-small.csv')

LOG_FILE = f'blast-logs/logs_{int(time.time() * 1000)}.csv'

# Example: "100,200,300..." -> [100, 200, 300, ...]
MESSAGES_PER_WINDOW_LIST = os.getenv('MESSAGES_PER_WINDOW', '100,200,300')
WAIT_BETWEEN_BLASTS = float(os.getenv('WAIT_BETWEEN_BLASTS', '2.0'))

# For example:
# MESSAGES_PER_WINDOW="100,300,500"
# WAIT_BETWEEN_BLASTS="2"

FIELD_CASTS = {
    'created_utc': int,
    'score_hidden': int,
    'glided': int,
    'ups': int,
    'downs': int,
    'archived': int,
    'score': int,
    'retrieved_on': int,
    'edited': int,
    'controversiality': int
}


def parse_messages_per_window_list(s):
    """
    Converts a comma-separated string of integers into a list of ints.
    """
    return [int(x.strip()) for x in s.strip().split(',')]


def json_serializer(data):
    """Converts a Python dictionary into a JSON-encoded bytes object."""
    return json.dumps(data).encode('utf-8')


def create_producer():
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=json_serializer,
            security_protocol='PLAINTEXT'
        )
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None


def preprocess_and_type_cast_fields(row: dict, field_casts: dict) -> dict:
    """
    For each field in 'field_casts', apply the cast function if the field exists in 'row'.
    """
    for field_name, cast_fn in field_casts.items():
        if field_name in row and row[field_name] is not None:
            try:
                row[field_name] = cast_fn(row[field_name])
            except ValueError:
                # Handle errors e.g., row[field_name] was not parseable
                print(f"Failed to cast field '{field_name}' with value '{row[field_name]}'")
                row[field_name] = None
    if "id" in row: # rename "id" to "id_" for compatibility
        row["id_"] = row["id"]
        row.pop("id", None)
    return row


def produce_blasts_from_csv():
    """
    Reads records from the CSV in 'blasts' of varying sizes.
    Each blast is sent back-to-back, then we wait SLEEP_BETWEEN_BLASTS sec.
    """
    producer = create_producer()
    if not producer:
        print("Failed to create Kafka producer. Exiting.")
        return

    # Parse messages-per-blast list from environment
    blasts_sizes = parse_messages_per_window_list(MESSAGES_PER_WINDOW_LIST)

    try:
        with open(REDDIT_CSV_FILE, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            total_records_sent = 0
            blast_id = 0
            start_time = time.time()

            # We'll keep reading the file until we run out of lines
            # or we've exhausted the needed blasts. We'll cycle through blasts_sizes repeatedly.
            while True:
                blast_size = blasts_sizes[blast_id % len(blasts_sizes)]
                blast_id += 1

                # Mark the real-time start for this blast
                blast_start_time = int(time.time() * 1000)
                print(f"\n[Blast {blast_id}] -> Sending {blast_size} records. Real-time start: {blast_start_time}")

                for i in range(blast_size):
                    try:
                        row = next(reader)
                    except StopIteration:
                        print("No more lines in CSV. Stopping.")
                        # Final flush and exit
                        producer.flush()
                        elapsed = time.time() - start_time
                        print(f"Done sending {total_records_sent} records. Elapsed time: {elapsed:.2f}s")
                        return

                    # Type-cast fields
                    row = preprocess_and_type_cast_fields(row, FIELD_CASTS)

                    # Add metadata about the blast (optional)
                    row["blast_id"] = blast_id
                    row["blast_start_time"] = blast_start_time

                    # Send to Kafka
                    producer.send(KAFKA_INPUT_TOPIC, row)
                    total_records_sent += 1

                blast_end_time = int(time.time() * 1000)

                # Flush after each blast
                producer.flush()

                # Optionally wait between blasts
                if WAIT_BETWEEN_BLASTS > 0:
                    for _ in range(1 if blast_size < 90000 else 3):
                        # print(f"[Blast {blast_id}] -> Sleeping {WAIT_BETWEEN_BLASTS:.2f}s before next blast.")
                        time.sleep(WAIT_BETWEEN_BLASTS)

                producer.send(KAFKA_OUTPUT_TOPIC, value={
                    'blast_id': blast_id, 'blast_size': blast_size,
                    'blast_start_time': blast_start_time, 'blast_end_time': blast_end_time}, key="blast".encode('utf-8'))
                time.sleep(3)


    except FileNotFoundError:
        print(f"CSV file '{REDDIT_CSV_FILE}' not found.")
    except Exception as e:
        print(f"Error while reading CSV or producing messages to Kafka: {repr(e)}")
    finally:
        producer.close()


def main():
    print(f"Starting replay at real-time: {time.time():.2f}")
    produce_blasts_from_csv()
    print(f"Finished replay at real-time: {time.time():.2f}")


if __name__ == "__main__":
    main()
