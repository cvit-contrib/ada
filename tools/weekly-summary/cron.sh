#!/bin/bash


WORKROOT='/home/jerin/admin/summary'
python3.4 $WORKROOT/summary.py --output $WORKROOT/tmp.csv
cat $WORKROOT/tmp.csv                   \
    | column -t -s ','                  \
    | head -n 20                        \
        > $WORKROOT/tmp.csv.head

cat $WORKROOT/tmp.csv.head                  \
    | mail                                  \
        -s "ada: usage Summary"             \
        -a $WORKROOT/tmp.csv                \
        jerin.philip@research.iiit.ac.in    
    

