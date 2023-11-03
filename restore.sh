#!/bin/bash

# Set the base directory where this script is located
BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"



# Initialize optional arguments
PREVIEW=false
CONFIG_FILE=""

# Create a classpath variable to include all JARs in the lib folder
CLASSPATH="${BASEDIR}/../lib/*"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --preview)
            PREVIEW=true
            ;;
        *)
            CONFIG_FILE="$1"
            ;;
    esac
    shift
done

# Check if a configuration file is provided
if [ -z "$CONFIG_FILE" ]; then
    echo "Error: Please specify the application configuration file."
    exit 1
fi

# Add optional logic for handling the --preview flag
if [ "$PREVIEW" = true ]; then
    echo "Running the application in preview mode with configuration file: $CONFIG_FILE"
    java -cp "$CLASSPATH" io.lenses.App --config "$CONFIG_FILE" --preview
else
    echo "Running the application with configuration file: $CONFIG_FILE"
    java -cp "$CLASSPATH" io.lenses.App --config "$CONFIG_FILE"
fi
