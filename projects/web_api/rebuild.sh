#!/bin/bash

sudo docker build -f Dockerfile -t tubo-pdf:latest .
sudo docker stop tubo-pdf
sudo docker rm tubo-pdf
sudo docker run --name tubo-pdf -p 8000:8000 --restart always tubo-pdf:latest

