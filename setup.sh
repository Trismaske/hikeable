#!/bin/bash

# Setup script for Hikeable
# This script sets up the Python environment and installs required packages

set -e  # Exit on any error

echo "Setting up Hikeable environment..."


# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "Virtual environment created"
else
    echo "Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install required packages
echo "Installing required packages..."
pip install -r requirements.txt

# Setup GCP ADC
gcloud auth application-default login
