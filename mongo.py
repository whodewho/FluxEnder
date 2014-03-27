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

def generate_sus_dict(_dict, content):
    sus_dict = {}
    sus_dict[content] = Set([])
    sus_dict["TTLS"] = Set([])
    sus_dict["FIRST_SEEN"] = 10000000000
    sus_dict["LAST_SEEN"] = 0
    sus_dict["COUNT"] = 0

    for key in _dict:
        sus_dict[content].add(key)
        for ttl in _dict[key]["TTLS"]:
            sus_dict["TTLS"].add(ttl)
        if sus_dict["FIRST_SEEN"] > _dict[key]["FIRST_SEEN"]: sus_dict["FIRST_SEEN"] = _dict[key]["FIRST_SEEN"]
        if sus_dict["LAST_SEEN"] < _dict[key]["LAST_SEEN"]: sus_dict["LAST_SEEN"] = _dict[key]["LAST_SEEN"]
        sus_dict["COUNT"] = sus_dict["COUNT"] + _dict[key]["COUNT"]
    return sus_dict

def merge_sus_dict(_dict, sus_doc, content):
    for item in sus_doc[content]:
        _dict[content].add(item)
    for ttl in sus_doc["TTLS"]:
        _dict["TTLS"].add(ttl)
    if sus_doc["FIRST_SEEN"] <  _dict["FIRST_SEEN"]: _dict["FIRST_SEEN"] = sus_doc["FIRST_SEEN"]
    if sus_doc["LAST_SEEN"] > _dict["LAST_SEEN"]: _dict["LAST_SEEN"] = sus_doc["LAST_SEEN"] 
    _dict["COUNT"] = _dict["COUNT"] + sus_doc["COUNT"]
        

def dump_dict(nor_coll, sus_coll, _dict, content): 
    for key1 in _dict:
        sus_doc = sus_coll.find_one({"_id":key1})
        if sus_doc != None:
            new_sus_dict = generate_sus_dict(_dict[key1], content)
            merge_sus_dict(new_sus_dict, sus_doc, content)

            sus_coll.update({"_id":key1}, {"$set":{content:list(new_sus_dict[content]), \
                                                "TTLS":list(new_sus_dict["TTLS"]), \
                                                "FIRST_SEEN":new_sus_dict["FIRST_SEEN"], \
                                                "LAST_SEEN":new_sus_dict["LAST_SEEN"], \
                                                "COUNT":new_sus_dict["COUNT"] \
                                                }})
        else:
            nor_doc = nor_coll.find_one({"_id":key1})
            if nor_doc == None:
                for key2 in _dict[key1]:
                    _dict[key1][key2]["TTLS"] = list(_dict[key1][key2]["TTLS"])
                nor_coll.insert({"_id":key1, content:json.dumps(_dict[key1])})
            else:
                mongo_dict = json.loads(nor_doc[content])
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

                if len(_dict[key1]) > 200:
                    new_sus_dict = generate_sus_dict(_dict[key1], content)

                    sus_coll.insert({"_id":key1, content:list(new_sus_dict[content]), \
                                                "TTLS":list(new_sus_dict["TTLS"]), \
                                                "FIRST_SEEN":new_sus_dict["FIRST_SEEN"], \
                                                "LAST_SEEN":new_sus_dict["LAST_SEEN"], \
                                                "COUNT":new_sus_dict["COUNT"], \
                                                })
                    nor_coll.remove({"_id":key1})
                else:
                    for key2 in _dict[key1]:
                        _dict[key1][key2]["TTLS"] = list(_dict[key1][key2]["TTLS"])
                    nor_coll.update({"_id":key1}, {"$set":{content:json.dumps(_dict[key1])}})

p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
client = MongoClient()
db = client.passiveDNS
domain_dict = {}
ip_dict = {}
file_list = []
sus_ip_set = Set([])
sus_domain_set = Set([])
nor_domain_set = Set([])
root = "/dnscap-data1/dnslog/"

for root, dirs, files in os.walk(root): 
    files.sort()
    for f in files:
        if not p.match(f):continue 
        this_date = datetime.strptime(f,"%Y-%m-%d").date() 
        if this_date > date(2013,9,13) and this_date < date(2013,10,1):
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
    dump_dict(db.domain, db.sus_domain, domain_dict, "IPS")
    print (time.clock() - bar2) / len(domain_dict)  
    domain_dict.clear()

    bar3 = time.clock()
    dump_dict(db.ip, db.sus_ip, ip_dict, "DOMAINS") 
    print(time.clock() - bar3) / len(ip_dict)
    ip_dict.clear()

    mongo_print([datetime.now()])

