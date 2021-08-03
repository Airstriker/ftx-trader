#!/bin/bash
# Start PC version of ftx_trader

source ./venv/bin/activate
python ftx_trader.py -c ./configs/ftx_trader.cfg_test
