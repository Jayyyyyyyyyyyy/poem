#!/bin/sh

ps -ef | grep python | grep runserver | grep 8903 |  awk -F' ' '{print $2}' | xargs kill -9
