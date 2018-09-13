#!/bin/bash

if [ "$1" == "start" ]; then
	# remove all old logs
	rm -r logs 2>/dev/null
	mkdir logs 2>/dev/null
	
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

if [ "$1" == "evaluate" ]; then
	# analyze log output
	egrep '\*\*\*\*\*\*\*\*\*\*\*' logs/full.log >logs/evaluation.log
	(
		echo "shorter_subscribers = int(\"$(grep 'SHORTER NONCE: subscriber' logs/evaluation.log | wc -l)\")"
		echo "shorter_intermediates = int(\"$(grep 'SHORTER NONCE: intermediate' logs/evaluation.log | wc -l)\")"
		echo "master_publishers = int(\"$(grep 'BECOMING NEW MASTER: ' logs/evaluation.log | wc -l)\")"
		echo "overlay_construction = []"
		grep '\*\*\*\*\*\*\*\*\*\*\* OVERLAY CONSTRUCTED: ' logs/evaluation.log | while read line; do
			echo "overlay_construction.append(float(\"$(echo "$line" | sed --unbuffered -e "s/^.*OVERLAY CONSTRUCTED: //")\"))"
		done
	) >logs/evaluation.py
	exit 0
fi

if [ "$1" == "cleanup" ]; then
	rm -r "$2"* 2>/dev/null
fi

echo $@
exit 0