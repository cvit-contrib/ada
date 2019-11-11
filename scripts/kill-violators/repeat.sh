#!/bin/bash

while :
do
    echo "#" $(date) ----------;
    ada violators | sed 's/HUP/KILL/g' > latest.txt
    cat latest.txt
    cat latest.txt | bash
    cat latest.txt >> log.txt
    sleep 15m; 
done;

