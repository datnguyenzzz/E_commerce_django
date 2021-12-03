#!/bin/bash

for i in {1..10} 
do
    NUMBER=$(cat /dev/urandom | tr -dc '0-9' | fold -w 256 | head -n 1 | sed -e 's/^0*//' | head --bytes 2)
    phrase=$(cat /dev/urandom | tr -dc "a-zA-Z0-9" | fold -w $NUMBER | head -n 1)
    TIME=$(cat /dev/urandom | tr -dc '0-9' | fold -w 256 | head -n 1 | sed -e 's/^0*//' | head --bytes 1)
    for (( c=1; c<=$TIME; c++ ))
    do
        curl -X POST http://localhost:8080/api/v1.0/gather\
        -H 'Content-Type: application/json'\
        -d '{"word":"'$phrase'","lang":"en"}'
    done

done
