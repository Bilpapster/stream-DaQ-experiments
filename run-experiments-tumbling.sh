#!/bin/bash

run_experiment() {
  local service="$1"
  local description="$2"

  echo "Starting experiment: $description"
  
  # Start Kafka and wait for it to be ready.
  docker compose up -d kafka &&
  sleep 5 &&
  docker exec kafka bash /app/scripts/kafka-entrypoint.sh &&

  # Start the chosen service (deequ-reddit or daq) and the stream.
  docker compose up -d "$service" &&
  sleep 5 &&
  docker compose up -d stream &&
  
  # Wait for the experiment duration (2100 seconds).
  sleep 2000 &&
  
  # Tear down all the services.
  docker compose down
}


# Define the memory limits and CPU (spark cores) limits you want to test.
memory_limits=("8G" "12G" "16G")
cpu_limits=("2" "4" "8")  # SPARK_NUM_CORES values
tumbling_windows=("1 minutes" "5 minutes" "30 minutes" "60 minutes")

# set the window type to sliding
sed -i "s/^WINDOW_TYPE=.*/WINDOW_TYPE=tumbling/" .env

# Loop over each memory and cpu combination.
for cpus in "${cpu_limits[@]}"; do
   for memory in "${memory_limits[@]}"; do
      echo "Configuring .env for MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus"
      for tumbling_window in "${tumbling_windows[@]}"; do
         # Update MEMORY_LIMIT
         sed -i "s/^MEMORY_LIMIT=.*/MEMORY_LIMIT=$memory/" .env

         # Update SPARK_NUM_CORES
         sed -i "s/^SPARK_NUM_CORES=.*/SPARK_NUM_CORES=$cpus/" .env

         # Update window duration
         sed -i "s/^WINDOW_DURATION=.*/WINDOW_DURATION=$tumbling_window/" .env

         # Run both service variations for the current configuration.
         for service in "deequ-reddit" "daq"; do
            description="Running: tumbling $tumbling_window with MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus, service: $service"
            run_experiment "$service" "$description"
         done
      done
   done
done

