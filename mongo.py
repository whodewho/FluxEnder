import pymongo
from pymongo import MongoClient
from datetime import date, time, datetime, timedelta
from sets import Set
import re
import sys
import os
import json
import time

def mongo_print(args):
    run_log_writer = open("run.log", "a")
    print str(args)
    run_log_writer.write(str(args)+"\n")
    run_log_writer.close()

def load_one_line(_dict, key1, key2, _list):
    if key1 not in _dict:
        _dict[key1] = {}
    if key2 not in _dict[key1]:
        _dict[key1][key2] = {}
        _dict[key1][key2]["TTLS"] = Set([int(_list[7])])
        _dict[key1][key2]["FIRST_SEEN"] = float(_list[0])
        _dict[key1][key2]["LAST_SEEN"] = float(_list[0])
        _dict[key1][key2]["COUNT"] = int(_list[8])
    else:
        _dict[key1][key2]["TTLS"].add(int(_list[7]))
        if _dict[key1][key2]["FIRST_SEEN"] > float(_list[0]): _dict[key1][key2]["FIRST_SEEN"] = float(_list[0])
        if _dict[key1][key2]["LAST_SEEN"] < float(_list[0]): _dict[key1][key2]["LAST_SEEN"] = float(_list[0])
        _dict[key1][key2]["COUNT"] = _dict[key1][key2]["COUNT"] + int(_list[8])

def dump_dict(collection, _dict, second_level): 
    for key1 in _dict:
        document = collection.find_one({"_id":key1})
        if document == None:
            for key2 in _dict[key1]:
                _dict[key1][key2]["TTLS"] = list(_dict[key1][key2]["TTLS"])
            collection.insert({"_id":key1, second_level:json.dumps(_dict[key1])})
        else:
            if len(json.dumps(document)) > 4000000:
                print key1, key2 
                continue
            mongo_dict = json.loads(document[second_level])
            for key2 in mongo_dict:
                if key2 not in _dict[key1]:
                    _dict[key1][key2] = mongo_dict[key2]
                else:
                    for ttl in mongo_dict[key2]["TTLS"]:
                        _dict[key1][key2]["TTLS"].add(ttl)
                    if mongo_dict[key2]["FIRST_SEEN"] < _dict[key1][key2]["FIRST_SEEN"]:
                        _dict[key1][key2]["FIRST_SEEN"] = mongo_dict[key2]["FIRST_SEEN"]
                    if mongo_dict[key2]["LAST_SEEN"] > _dict[key1][key2]["LAST_SEEN"]:
                        _dict[key1][key2]["LAST_SEEN"] = mongo_dict[key2]["LAST_SEEN"]
                    _dict[key1][key2]["COUNT"] = _dict[key1][key2]["COUNT"] + mongo_dict[key2]["COUNT"]
            for key2 in _dict[key1]:
                _dict[key1][key2]["TTLS"] = list(_dict[key1][key2]["TTLS"])
            collection.update({"_id":key1}, {"$set":{second_level:json.dumps(_dict[key1])}})

p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
client = MongoClient()
db = client.passiveDNS
domain_dict = {}
ip_dict = {}
file_list = []
sus_ip_set = Set([])
sus_domain_set = Set([])
root = "/dnscap-data1/dnslog/"

for root, dirs, files in os.walk(root): 
    files.sort()
    for f in files:
        if not p.match(f):continue 
        this_date = datetime.strptime(f,"%Y-%m-%d").date() 
        if this_date > date(2013,9,22) and this_date < date(2014,2,27):
            file_list.append(f)

for f in file_list:
    mongo_print([f, datetime.now()])

    bar1 = time.clock()
    number = 0
    for line in open(root + f):
        number = number + 1
        line_array = line.split("||")

        if len(line_array) != 9 or line_array[5] != 'A' : continue
        
        domain = (line_array[4][:len(line_array[4])-1]).lower()
        ip = line_array[6]
        load_one_line(domain_dict, domain, ip, line_array)
        load_one_line(ip_dict, ip, domain, line_array)
        
    print (time.clock() - bar1) / number     
    mongo_print([len(domain_dict), len(ip_dict), number, datetime.now()])

    bar2 = time.clock()
    dump_dict(db.domain, domain_dict, "IPS")
    print (time.clock() - bar2) / len(domain_dict)  
    domain_dict.clear()

    bar3 = time.clock()
    dump_dict(db.ip, ip_dict, "DOMAINS") 
    print(time.clock() - bar3) / len(ip_dict)
    ip_dict.clear()

    mongo_print([datetime.now()])

