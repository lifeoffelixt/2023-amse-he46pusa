#!/bin/bash

pip install -r project/requirements.txt

# Run the pytest script
python -m pytest project/test/test_ETLPipeline.py -v

