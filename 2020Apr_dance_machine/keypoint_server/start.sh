#!/bin/sh
nohup /usr/bin/python3  manage.py runserver 0.0.0.0:8900  --noreload  >>nohupout.log 2>&1  &
