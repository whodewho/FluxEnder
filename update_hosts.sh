#!/bin/bash
#wget http://dns-bh.sagadc.org/justdomains -O hosts_malwaredomains.txt
wget http://mirror1.malwaredomains.com/files/domains.txt -O tmp.txt
cat tmp.txt | grep -v '#' | grep -e 'fastflux' -e 'bot' | awk '{print $1}' > hosts_malwaredomains.txt
rm tmp.txt


#wget http://www.malwaredomainlist.com/hostslist/hosts.txt -tmp.txt
#cat tmp.txt | grep 127.0.0.1 | grep -v localhost | awk '{print $2}' > hosts_malwaredomainlist.txt
#rm tmp.txt


#wget http://hphosts.gt500.org/hosts.txt -O tmp.txt
#cat tmp.txt | grep 127.0.0.1 | grep -v localhost | awk '{print $2}' > hosts_hphosts.txt
#rm tmp.txt


wget https://zeustracker.abuse.ch/blocklist.php?download=baddomains -O tmp.txt
cat  tmp.txt | grep -v '#' | grep -v localhost | grep -v "^$" > hosts_badZeuS.txt
rm tmp.txt
#
#wget https://zeustracker.abuse.ch/blocklist.php?download=domainblocklist -O tmp.txt
#cat  tmp.txt | grep -v '#' | grep -v localhost | grep -v "^$" > hosts_stdZeuS.txt
#rm tmp.txt


#wget http://data.phishtank.com/data/online-valid.csv -O hosts_phishtank.csv


#wget http://cybercrime-tracker.net/all.php -O tmp.txt
#cat tmp.txt | sed 's/<br\ \/>/\n/g' > hosts_CyberCrime.txt
#rm tmp.txt