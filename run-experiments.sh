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
  sleep 2400 &&
  
  # Tear down all the services.
  docker compose down
}


#sed -i "s/^WINDOW_TYPE=.*/WINDOW_TYPE='tumbling'/" .env
#sed -i "s/^WINDOW_DURATION=.*/WINDOW_DURATION='1 minute'/" .env

# Define the memory limits and CPU (spark cores) limits you want to test.
memory_limits=("8G" "12G" "16G")
cpu_limits=("2" "4" "8")  # SPARK_NUM_CORES values

# Loop over each memory and cpu combination.
for cpus in "${cpu_limits[@]}"; do
   for memory in "${memory_limits[@]}"; do
       echo "Configuring .env for MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus"
       
       # Copy the default settings into .env and update only the two parameters.
       #  cp default.env .env
       
       # Update MEMORY_LIMIT
       sed -i "s/^MEMORY_LIMIT=.*/MEMORY_LIMIT=$memory/" .env
       
       # Update SPARK_NUM_CORES
       sed -i "s/^SPARK_NUM_CORES=.*/SPARK_NUM_CORES=$cpus/" .env
       
       # Run both service variations for the current configuration.
       for service in "deequ-reddit" "daq"; do
         description="tumbling 10 window with MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus, service: $service"
         run_experiment "$service" "$description"
       done
   done
done

