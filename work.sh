#!/bin/bash

datebegin=$1
dateend=$2
beg_s=`date -d "$datebegin" +%s`
end_s=`date -d "$dateend" +%s`
day_gap=1

while [ "$beg_s" -lt "$end_s" ]
do
    beg_f=$(date -d "@$beg_s" "+%g%m%d")
    python mongo.py $beg_f $day_gap
    beg_s=$((beg_s+86400*day_gap))
done

