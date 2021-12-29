#!/bin/bash

make init 

#init data for the first time
make init_data 
sleep 5
make migrate

while :
do  
    # simulate user request 
    make bulk_request
    # migrate new data every 30 min
    sleep 30 * 60
    make migrate
done