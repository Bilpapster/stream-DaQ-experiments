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
memory_limits=("16G" "12G" "8G")
cpu_limits=("8" "4" "2")  # SPARK_NUM_CORES values
sliding_windows=(
   #  "1 minutes/2 seconds"   "1 minutes/6 seconds"   "1 minutes/30 seconds"
    "5 minutes/2.5 minutes" "5 minutes/0.5 minutes"
    "10 minutes/5 minutes"  "10 minutes/1 minutes"
    "30 minutes/15 minutes" "30 minutes/3 minutes"
)

# set the window type to sliding
sed -i "s/^WINDOW_TYPE=.*/WINDOW_TYPE=sliding/" .env

# Loop over each memory and cpu combination.
for cpus in "${cpu_limits[@]}"; do
   for memory in "${memory_limits[@]}"; do
      echo "Configuring .env for MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus"
      for sliding_window in "${sliding_windows[@]}"; do
         # Update MEMORY_LIMIT
         sed -i "s/^MEMORY_LIMIT=.*/MEMORY_LIMIT=$memory/" .env

         # Update SPARK_NUM_CORES
         sed -i "s/^SPARK_NUM_CORES=.*/SPARK_NUM_CORES=$cpus/" .env

         # read and extract the sliding window duration and slide
         IFS='/'
         read -ra duration_slide <<< "$sliding_window"
         duration=${duration_slide[0]}
         slide=${duration_slide[1]}

         # Update window duration and slide
         sed -i "s/^WINDOW_DURATION=.*/WINDOW_DURATION=$duration/" .env
         sed -i "s/^SLIDE_DURATION=.*/SLIDE_DURATION=$slide/" .env
         

         # Run both service variations for the current configuration.
         for service in "deequ-reddit" "daq"; do
            description="Running: sliding $duration/$slide window with MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus, service: $service"
            run_experiment "$service" "$description"
         done
      done
   done
done

