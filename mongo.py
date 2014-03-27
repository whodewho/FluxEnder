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

def generate_dict(_dict, content):
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

def merge_dict(_dict, _doc, content):
    if _doc == None: return

    for item in _doc[content]:
        _dict[content].add(item)
    for ttl in _doc["TTLS"]:
        _dict["TTLS"].add(ttl)
    if _doc["FIRST_SEEN"] <  _dict["FIRST_SEEN"]: _dict["FIRST_SEEN"] = _doc["FIRST_SEEN"]
    if _doc["LAST_SEEN"] > _dict["LAST_SEEN"]: _dict["LAST_SEEN"] = _doc["LAST_SEEN"] 
    _dict["COUNT"] = _dict["COUNT"] + _doc["COUNT"]

def update_coll(_coll, key, _dict, content):
    _coll.update({"_id":key}, {"$set":{content:list(_dict[content]), "TTLS":list(_dict["TTLS"]), \
                                    "FIRST_SEEN":_dict["FIRST_SEEN"], "LAST_SEEN":_dict["LAST_SEEN"], \
                                    "COUNT":_dict["COUNT"] \
                                    }}, True)

def insert_coll(_coll, key, _dict, content):
    _coll.insert({"_id":key, content:list(_dict[content]), "TTLS":list(_dict["TTLS"]), \
                                    "FIRST_SEEN":_dict["FIRST_SEEN"], "LAST_SEEN":_dict["LAST_SEEN"], \
                                    "COUNT":_dict["COUNT"], \
                                })

def is_in_nor_set(nor_set, key):
    for item in nor_set:
        if key.endswith(item):
            return True
    return False

def dump_dict(_coll, nor_coll, sus_coll, nor_set, sus_set, _dict, content): 
    for key1 in _dict:
        if key1 in sus_set:
            new_sus_dict = generate_dict(_dict[key1], content)
            sus_doc = sus_coll.find_one({"_id":key1})
            merge_dict(new_sus_dict, sus_doc, content)
            update_coll(sus_coll, key1, new_sus_dict, content)
        elif is_in_nor_set(nor_set, key1):
            new_nor_dict = generate_dict(_dict[key1], content)
            nor_doc = nor_coll.find_one({"_id":key1})
            merge_dict(new_nor_dict, nor_doc, content) 
            update_coll(nor_coll, key1, new_nor_dict, content)
        else:
            _doc = _coll.find_one({"_id":key1})
            if _doc == None:
                for key2 in _dict[key1]:
                    _dict[key1][key2]["TTLS"] = list(_dict[key1][key2]["TTLS"])
                _coll.insert({"_id":key1, content:json.dumps(_dict[key1])})
            else:
                mongo_dict = json.loads(_doc[content])
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
                    new_sus_dict = generate_dict(_dict[key1], content)
                    insert_coll(sus_coll, key1, new_sus_dict, content)
                    sus_set.add(key1)
                    _coll.remove({"_id":key1})
                else:
                    for key2 in _dict[key1]:
                        _dict[key1][key2]["TTLS"] = list(_dict[key1][key2]["TTLS"])
                    _coll.update({"_id":key1}, {"$set":{content:json.dumps(_dict[key1])}})

def init_sus_set(coll, sus_set):
    cursor = coll.find({},{"_id":1})
    for item in cursor:
        sus_set.add(item["_id"])

def init_nor_set(f, nor_set):
    for line in open(f):
        nor_set.add(line.strip('\n'))


#--------------Main-----------------#
p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
client = MongoClient()
db = client.passiveDNS

file_list = []
ip_dict = {}
domain_dict = {}

sus_ip_set = Set([])
init_sus_set(db.sus_ip, sus_ip_set) 
sus_domain_set = Set([])
init_sus_set(db.sus_domain, sus_domain_set) 

nor_ip_set = Set([])
nor_domain_set = Set([])
init_nor_set("domain_whitelist.txt", nor_domain_set) 

root = "/dnscap-data1/dnslog/"

for root, dirs, files in os.walk(root): 
    files.sort()
    for f in files:
        if not p.match(f):continue 
        this_date = datetime.strptime(f,"%Y-%m-%d").date() 
        if this_date > date(2013,9,13) and this_date < date(2013, 10, 1):
            file_list.append(f)

for f in file_list:
    mongo_print(f)  
    mongo_print(datetime.now())

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
        
    mongo_print((time.clock() - bar1) / number)     
    mongo_print([len(domain_dict), len(ip_dict), number])
    mongo_print(datetime.now())

    bar2 = time.clock()
    dump_dict(db.domain, db.nor_domain, db.sus_domain, nor_domain_set, sus_domain_set, domain_dict, "IPS")
    mongo_print((time.clock() - bar2) / len(domain_dict))  
    mongo_print(datetime.now())
    domain_dict.clear()


    bar3 = time.clock()
    dump_dict(db.ip, db.nor_ip, db.sus_ip, nor_ip_set, sus_ip_set, ip_dict, "DOMAINS") 
    mongo_print((time.clock() - bar3) / len(ip_dict))
    mongo_print(str(datetime.now()) + "\n")
    ip_dict.clear()


