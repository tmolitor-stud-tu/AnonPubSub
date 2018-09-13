#!/bin/bash


for ip in {100..199}; do
	ip="127.0.0.$ip"
	./main.py -l $ip 2>&1 | tee -a "logs/$ip.log" | sed --unbuffered -e "s/^/$ip: /" &
done | tee -a logs/full.log
