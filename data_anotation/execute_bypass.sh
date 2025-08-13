#!/bin/sh

# Number of times to execute the script
ntimes=50

# Loop to execute the script
for i in $(seq 1 $ntimes); do
    ./recaptcha_bypass.sh
    sleep 2 #wait two second
done
