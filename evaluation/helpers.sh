#!/bin/bash

if [ "$1" == "start" ]; then
	# start nodes
	while read ip; do
		(cd ..; ./main.py -l "$ip" 2>&1 & echo "$!" >"evaluation/logs/$ip.pid") | tee -a "logs/$ip.log" | sed --unbuffered -e "s/^/$ip: /" &
	done | tee -a logs/full.log >/dev/null &
	exit 0
fi

if [ "$1" == "stop" ]; then
	# kill nodes
	for pidfile in logs/*.pid; do
		kill $(<$pidfile)
		rm $pidfile 2>/dev/null
	done
	exit 0
fi

exit 0