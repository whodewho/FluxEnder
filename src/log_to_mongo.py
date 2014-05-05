from sys import argv
from datetime import timedelta
import re
import os
import time
from pymongo import MongoClient
from log_to_mongo_lib import *


script, start_date_str, day_gap = argv
dns_log_path = "/home/kai/Workspace/graduation/dnscap-data1/dnslog/"
p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
start_datetime = datetime.strptime(start_date_str, "%y%m%d")
end_datetime = start_datetime + timedelta(days=int(day_gap))
client = MongoClient()
db = client["p" + start_date_str]

ip_dict = {}
domain_dict = {}
sus_ip_set = set([])
nor_ip_set = set([])

outlier_domain_set = init_outlier_domain_set()
nor_domain_set = init_from_alexa(5000, 10000)
sus_domain_set = init_sus_domain_set()

domain_cache = init_cache()
ip_cache = init_cache()
sus_domain_cache = init_cache()
sus_ip_cache = init_cache()
nor_domain_cache = init_cache()
nor_ip_cache = init_cache()
spe_domain_cache = init_cache()
spe_ip_cache = init_cache()

mongo_print("\n\n\n\n\n" + str(os.path.basename(__file__)))

file_list = []
for root, dirs, files in os.walk(dns_log_path):
    files.sort()
    for f in files:
        if not p.match(f):
            continue
        this_date = datetime.strptime(f, "%Y-%m-%d").date()
        if start_datetime.date() <= this_date < end_datetime.date():
            file_list.append(f)
print file_list

for f in file_list:
    mongo_print(f)
    mongo_print(datetime.now())

    bar1 = time.clock()
    number = 0
    for line in open(os.path.join(dns_log_path, f)):
        number += 1
        line_array = line.split("||")

        if len(line_array) != 9 or line_array[5] != 'A':
            continue

        domain = (line_array[4][:len(line_array[4]) - 1]).lower()
        ext = tldextract.extract(domain)
        subdomain = ext.subdomain
        if ext.domain == "":
            domain = ext.suffix
        else:
            domain = ".".join(ext[1:])

        if domain in outlier_domain_set:
            continue
        if domain in nor_domain_set and domain in sus_domain_set:
            continue
        if domain not in sus_domain_set and domain not in nor_domain_set:
            continue

        timestamp = float(line_array[0])
        ip = line_array[6]
        ttl = int(line_array[7])
        count = int(line_array[8])

        load_one_line(domain_dict, timestamp, subdomain, domain, ip, ttl, count)
        load_one_line(ip_dict, timestamp, subdomain, ip, domain, ttl, count)

    mongo_print([len(domain_dict), len(ip_dict), number])
    mongo_print((time.clock() - bar1) / number)
    mongo_print(datetime.now())

    bar2 = time.clock()
    dump_domain_dict(db.domain, db.nor_domain, db.sus_domain, db.spe_domain,
                     nor_domain_set, sus_domain_set,
                     domain_dict,
                     domain_cache, nor_domain_cache, sus_domain_cache, spe_domain_cache,
                     nor_ip_set, sus_ip_set)
    mongo_print((time.clock() - bar2) / len(domain_dict))
    mongo_print(datetime.now())
    domain_dict.clear()

    print len(nor_ip_set)
    print len(sus_ip_set)

    bar3 = time.clock()
    dump_ip_dict(db.ip, db.nor_ip, db.sus_ip, db.spe_ip,
                 nor_ip_set, sus_ip_set,
                 ip_dict,
                 ip_cache, nor_ip_cache, sus_ip_cache, spe_ip_cache)
    mongo_print((time.clock() - bar3) / len(ip_dict))
    mongo_print(str(datetime.now()) + "\n")
    ip_dict.clear()


