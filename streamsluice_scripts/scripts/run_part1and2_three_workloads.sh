#!/bin/bash

# List of your script files
scripts=(
  # "linear-road.sh"
  "stock-analysis.sh"
  "twitter-alert.sh"
  # Add more scripts as needed
)

# Function to run each script sequentially
run_scripts_sequentially() {
  for script in "${scripts[@]}"; do
    echo "Running $script..."
    bash $script
    if [ $? -eq 0 ]; then
      echo "$script finished successfully."
    else
      echo "Error: $script encountered an issue."
      exit 1
    fi
    echo "-------------------------------------"
  done
  echo "All scripts have been executed."
}

# Run the function
run_scripts_sequentially
