#!/bin/bash

# Create a Conda environment from conda.yaml
conda env create --name testing_SAMSE_env --file project/conda.yaml

# Activate the Conda environment
conda activate testing_SAMSE_env

# Install pytest (if not already installed)
pip install pytest

# Run the pytest script
python -m pytest project/test/test_ETLPipeline.py -v

# Deactivate the Conda environment
conda deactivate

# Delete the Conda environment
conda env remove --name testing_SAMSE_env
