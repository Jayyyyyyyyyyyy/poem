#!/bin/sh

ps -ef | grep python | grep runserver | grep 8905 |  awk -F' ' '{print $2}' | xargs kill -9
