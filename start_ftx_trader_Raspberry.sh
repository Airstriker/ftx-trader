#!/bin/bash
# Start Raspberry version of ftx_trader

source ./venv/bin/activate
python ftx_trader.py -r -c ./configs/ftx_trader.cfg_test
