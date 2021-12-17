#!/bin/sh
nohup python  manage.py runserver 0.0.0.0:8903  --noreload  >>nohupout.log 2>&1  &

