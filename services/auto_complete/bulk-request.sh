#!/bin/bash

curl -X POST http://localhost:8080/api/v1.0/gather\
     -H 'Content-Type: application/json'\
     -d '{"word":"dfhjgasdhjgasdkj","lang":"en"}'
