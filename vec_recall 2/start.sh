#!/bin/bash

log_dir="logs"

if [ ! -d "$log_dir" ]; then
        mkdir $log_dir
fi

FLASK_APP=similarity/qsim.py nohup flask run --host '0.0.0.0' --port 5012 > running.log 2>&1 &