#!/bin/bash
#wget http://dns-bh.sagadc.org/justdomains -O hosts_malwaredomains.txt
#
#
#wget http://www.malwaredomainlist.com/hostslist/hosts.txt
#cat hosts.txt | grep 127.0.0.1 | grep -v localhost | awk '{print $2}' > tmp.txt
#mv tmp.txt hosts_malwaredomainlist.txt
# rm hosts.txt


#wget http://hphosts.gt500.org/hosts.txt
#cat hosts.txt | grep 127.0.0.1 | grep -v localhost | awk '{print $2}' > tmp.txt
#mv tmp.txt hosts_hphosts.txt
#rm hosts.txt


wget https://zeustracker.abuse.ch/blocklist.php?download=baddomains -O tmp.txt
cat  tmp.txt | grep -v '#' | grep -v localhost | grep -v "^$" > hosts_badZeuS.txt
rm tmp.txt

wget https://zeustracker.abuse.ch/blocklist.php?download=domainblocklist -O tmp.txt
cat  tmp.txt | grep -v '#' | grep -v localhost | grep -v "^$" > hosts_stdZeuS.txt
rm tmp.txt


#wget http://data.phishtank.com/data/online-valid.csv -O hosts_phishtank.csv
