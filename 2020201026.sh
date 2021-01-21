#!/bin/bash

# Usage: ./2020201026.sh "<query>"
# Dataset files sould be stored in /files

python 2020201026_sqlengine.py --query "$1"
