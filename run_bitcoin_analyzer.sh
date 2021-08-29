#! /bin/bash

pip install virtualenv

python -m venv bitcoin-venv

source bitcoin-venv/bin/activate

pip install -r requirements.txt

python ./src/bit_coin_analysis.py