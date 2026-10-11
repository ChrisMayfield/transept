#!/bin/bash
#
# This script checks the backend server for various rooms and endpoints,
# simulating different scenarios such as missing tokens and valid requests.
# It uses curl to make HTTP requests and outputs the results for each room.

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 public_url reader_token control_token room" >&2
    exit 1
fi

HOST=$1
READER=$2
CONTROL=$3
ROOM=$4

BLUE='\033[34m%s\033[0m\n'
note() {
    printf "$BLUE" "$1"
}

printf '\n'
note "404, the bot that finds it"
curl -sI "$HOST/$ROOM"

note "403, no token"
curl -s  "$HOST/$ROOM/api/channels"
printf '\n\n'

note "Correct token, should be 200"
curl -s  "$HOST/$ROOM/api/channels?token=$READER"
printf '\n\n'

note "Correct token, wrong origin"
curl -s -H "Origin: https://example.com" "$HOST/$ROOM/control?token=$CONTROL"
printf '\n\n'
