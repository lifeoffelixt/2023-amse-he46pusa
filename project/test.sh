#!/bin/bash

pip install -r project/test/test_requirements.txt

# Run the pytest script
python -m pytest project/test/test_ETLPipeline.py -v

