#!/bin/bash

# Parar Payment Processors
docker-compose -f docker-compose-payment-processor.yml down

# Subir aplicação
docker-compose -f docker-compose.yml down

docker container prune | y

# Build da aplicação
./mvnw clean package -Pnative spring-boot:build-image