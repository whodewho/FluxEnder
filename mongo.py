import pymongo
from pymongo import MongoClient
from datetime import date, time, datetime, timedelta
from sets import Set
import re
import sys
import os
import json
import time

p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
client = MongoClient()
db = client.passiveDNS
domain_dict = {}
ip_dict = {}
file_list = []
root = "/dnscap-data1/dnslog/"
run_log_writer = open("run.log", "a")

for root, dirs, files in os.walk(root): 
    files.sort()
    for f in files:
        if not p.match(f):continue 
        this_date = datetime.strptime(f,"%Y-%m-%d").date() 
        if this_date > date(2013,9,22) and this_date < date(2014,2,27):
            file_list.append(f)

for f in file_list:
    print f, "\n", datetime.now()
    run_log_writer.write(f + "\n" + str(datetime.now()) + "\n")

    bar1 = time.clock()
    number = 0
    for line in open(root + f):
        number = number + 1
        line_array = line.split("||")

        if len(line_array) != 9 or line_array[5] != 'A' : continue

        domain = (line_array[4][:len(line_array[4])-1]).lower()
        ip = line_array[6]

        if domain not in domain_dict:
            domain_dict[domain]={}
        if ip not in domain_dict[domain]:
            domain_dict[domain][ip]={}
            domain_dict[domain][ip]["TTLS"] = Set([int(line_array[7])])
            domain_dict[domain][ip]["FIRST_SEEN"] = float(line_array[0])
            domain_dict[domain][ip]["LAST_SEEN"] = float(line_array[0])
            domain_dict[domain][ip]["COUNT"] = int(line_array[8])
        else:
            domain_dict[domain][ip]["TTLS"].add(int(line_array[7]))
            if domain_dict[domain][ip]["FIRST_SEEN"] > float(line_array[0]): domain_dict[domain][ip]["FIRST_SEEN"] = float(line_array[0])
            if domain_dict[domain][ip]["LAST_SEEN"] < float(line_array[0]): domain_dict[domain][ip]["LAST_SEEN"] = float(line_array[0])
            domain_dict[domain][ip]["COUNT"] = domain_dict[domain][ip]["COUNT"] + int(line_array[8])

        if ip not in ip_dict:
            ip_dict[ip]={}
        if domain not in ip_dict[ip]:
            ip_dict[ip][domain]={}
            ip_dict[ip][domain]["TTLS"] = Set([int(line_array[7])])
            ip_dict[ip][domain]["FIRST_SEEN"] = float(line_array[0])
            ip_dict[ip][domain]["LAST_SEEN"] = float(line_array[0])
            ip_dict[ip][domain]["COUNT"] = int(line_array[8])
        else:
            ip_dict[ip][domain]["TTLS"].add(int(line_array[7]))
            if ip_dict[ip][domain]["FIRST_SEEN"] > float(line_array[0]):ip_dict[ip][domain]["FIRST_SEEN"] = float(line_array[0])
            if ip_dict[ip][domain]["LAST_SEEN"] < float(line_array[0]):ip_dict[ip][domain]["LAST_SEEN"] = float(line_array[0])
            ip_dict[ip][domain]["COUNT"] = ip_dict[ip][domain]["COUNT"] + int(line_array[8])
        
    print (time.clock() - bar1) / number     
    print len(domain_dict), len(ip_dict), number, datetime.now()
    run_log_writer.write(str(len(domain_dict)) + ", " + str(len(ip_dict)) + ", " + str(number) + str(datetime.now()) +"\n")

    bar2 = time.clock()
    number = 0
    for domain in domain_dict:
        number = number + 1
        document = db.domain.find_one({"_id":domain})
        if document == None:
            for ip in domain_dict[domain]:
                domain_dict[domain][ip]["TTLS"]=list(domain_dict[domain][ip]["TTLS"])
            db.domain.insert({"_id":domain, "IPS":json.dumps(domain_dict[domain])})
        else:
            if len(json.dumps(document)) > 4000000:
                print ip
                continue
            ips_document_dict = json.loads(document["IPS"])
            for ip_db in ips_document_dict:
                if ip_db not in domain_dict[domain]:
                    domain_dict[domain][ip_db] = ips_document_dict[ip_db]
                else:
                    for ip_db_ttl in ips_document_dict[ip_db]["TTLS"]:
                        domain_dict[domain][ip_db]["TTLS"].add(ip_db_ttl)
                    if ips_document_dict[ip_db]["FIRST_SEEN"] < domain_dict[domain][ip_db]["FIRST_SEEN"]:
                        domain_dict[domain][ip_db]["FIRST_SEEN"] = ips_document_dict[ip_db]["FIRST_SEEN"]
                    if ips_document_dict[ip_db]["LAST_SEEN"] > domain_dict[domain][ip_db]["LAST_SEEN"]:
                        domain_dict[domain][ip_db]["LAST_SEEN"] = ips_document_dict[ip_db]["LAST_SEEN"]
                    domain_dict[domain][ip_db]["COUNT"] = domain_dict[domain][ip_db]["COUNT"] + ips_document_dict[ip_db]["COUNT"]
            for ip in domain_dict[domain]:
                domain_dict[domain][ip]["TTLS"] = list(domain_dict[domain][ip]["TTLS"])
            db.domain.update({"_id":domain}, {"$set":{"IPS":json.dumps(domain_dict[domain])}})	
    domain_dict.clear()
    print (time.clock() - bar2) / number

    bar3 = time.clock()
    number = 0
    for ip in ip_dict:
        number = number + 1
        document = db.ip.find_one({"_id":ip})
        if document == None:
            for domain in ip_dict[ip]:
                ip_dict[ip][domain]["TTLS"]=list(ip_dict[ip][domain]["TTLS"])
            db.ip.insert({"_id":ip, "DOMAINS":json.dumps(ip_dict[ip])})
        else:
            if len(json.dumps(document)) > 4000000: 
                print ip
                continue
            domains_document_dict = json.loads(document["DOMAINS"])
            for domain_db in domains_document_dict:
                if domain_db not in ip_dict[ip]:
                    ip_dict[ip][domain_db] = domains_document_dict[domain_db]
                else:
                    for domain_db_ttl in domains_document_dict[domain_db]["TTLS"]:
                        ip_dict[ip][domain_db]["TTLS"].add(domain_db_ttl)
                    if domains_document_dict[domain_db]["FIRST_SEEN"] < ip_dict[ip][domain_db]["FIRST_SEEN"]:
                        ip_dict[ip][domain_db]["FIRST_SEEN"] = domains_document_dict[domain_db]["FIRST_SEEN"]
                    if domains_document_dict[domain_db]["LAST_SEEN"] > ip_dict[ip][domain_db]["LAST_SEEN"]:
                        ip_dict[ip][domain_db]["LAST_SEEN"] = domains_document_dict[domain_db]["LAST_SEEN"]
                    ip_dict[ip][domain_db]["COUNT"] = ip_dict[ip][domain_db]["COUNT"] + domains_document_dict[domain_db]["COUNT"] 
            for domain in ip_dict[ip]:
                ip_dict[ip][domain]["TTLS"] = list(ip_dict[ip][domain]["TTLS"])
            db.ip.update({"_id":ip},{"$set":{"DOMAINS":json.dumps(ip_dict[ip])}})
    ip_dict.clear()
    print(time.clock() - bar3) / number
    print datetime.now() +"\n"
    run_log_writer.write(str(datetime.now()) + "\n\n")
run_log.writer.close()
