#!/bin/bash
clear

cd ..
docker compose -f docker-compose-payment-processor.yml down
docker compose down

docker compose -f docker-compose-payment-processor.yml up -d
docker compose up -d

sleep 3

k6 run -e MAX_REQUESTS=550 rinha-test/rinha.js
#k6 run -e MAX_REQUESTS=550 rinha-test/rinha.js
#k6 run -e MAX_REQUESTS=550 rinha-test/rinha.js

docker compose -f docker-compose-payment-processor.yml down
docker compose down
