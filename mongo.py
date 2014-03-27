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

def transform_dict(_dict):
    result_dict = {}
    result_dict["ITEMS"] = Set([])
    result_dict["TTLS"] = Set([])
    result_dict["FIRST_SEEN"] = 10000000000
    result_dict["LAST_SEEN"] = 0
    result_dict["COUNT"] = 0

    for key in _dict:
        result_dict["ITEMS"].add(key)
        for ttl in _dict[key]["TTLS"]:
            result_dict["TTLS"].add(ttl)
        if result_dict["FIRST_SEEN"] > _dict[key]["FIRST_SEEN"]: result_dict["FIRST_SEEN"] = _dict[key]["FIRST_SEEN"]
        if result_dict["LAST_SEEN"] < _dict[key]["LAST_SEEN"]: result_dict["LAST_SEEN"] = _dict[key]["LAST_SEEN"]
        result_dict["COUNT"] = result_dict["COUNT"] + _dict[key]["COUNT"]
    return result_dict

def transform_doc(_doc):
    result_dict = {}
    result_dict["ITEMS"] = Set([])
    result_dict["TTLS"] = Set([])
    result_dict["FIRST_SEEN"] = 10000000000
    result_dict["LAST_SEEN"] = 0
    result_dict["COUNT"] = 0

    for item in _doc:
        result_dict["ITEMS"].add(item["ITEM"])
        for ttl in item["TTLS"]:
            result_dict["TTLS"].add(ttl)
        if result_dict["FIRST_SEEN"] > item["FIRST_SEEN"]: result_dict["FIRST_SEEN"] = item["FIRST_SEEN"]
        if result_dict["LAST_SEEN"] < item["LAST_SEEN"]: result_dict["LAST_SEEN"] = item["LAST_SEEN"]
        result_dict["COUNT"] = result_dict["COUNT"] + item["COUNT"]
    return result_dict

def insert_nor_sus(_coll, key, _dict ):
    _coll.insert({"_id":key, "ITEMS":list(_dict["ITEMS"]), "TTLS":list(_dict["TTLS"]), \
                                    "FIRST_SEEN":_dict["FIRST_SEEN"], "LAST_SEEN":_dict["LAST_SEEN"], \
                                    "COUNT":_dict["COUNT"], \
                                })

def update_nor_sus(_coll, key, _dict, _doc):
    if _doc == None:
        insert_nor_sus(_coll, key, _dict)
    else:
        new_item_list = []
        for item in _dict["ITEMS"]:
            if item not in _doc["ITEMS"]:
                new_item_list.append(item)
        if len(new_item_list) > 0:
            _coll.update({"_id":key},{"$addToSet":{"ITEMS":{"$each":new_item_list}}})
        new_ttl_list = []
        for ttl in _dict["TTLS"]:
            if ttl not in _doc["TTLS"]:
                new_ttl_list.append(item)
        if len(new_ttl_list) > 0:
            _coll.update({"_id":key},{"$addToSet":{"TTLS":{"$each":new_ttl_list}}})
        if _dict["FIRST_SEEN"] < _doc["FIRST_SEEN"]: 
            _coll.update({"_id":key}, {"$set":{"FIRST_SEEN":_dict["FIRST_SEEN"]}})
        if _dict["LAST_SEEN"] > _doc["LAST_SEEN"]: 
            _coll.update({"_id":key}, {"$set":{"LAST_SEEN":_dict["LAST_SEEN"]}})
        _coll.update({"_id":key}, {"$set":{"COUNT":_doc["COUNT"]+_dict["COUNT"]}})

def is_in_set(nor_set, key):
    key_array = key.split('.')
    l = len(key_array)
    if l < 2: return False
    tmp_key = key_array[l-2] + "." + key_array[l-1] 
    if tmp_key in nor_set: return True
    return False

def generate_list(_dict):
    result_list = [] 
    for item in _dict:
        tmp_dict = {}
        tmp_dict["ITEM"] = item
        tmp_dict["TTLS"] = _dict[item]["TTLS"]
        tmp_dict["FIRST_SEEN"] = _dict[item]["FIRST_SEEN"]
        tmp_dict["LAST_SEEN"] = _dict[item]["LAST_SEEN"]
        tmp_dict["COUNT"] = _dict[item]["COUNT"]
        result_list.append(tmp_dict)
    return result_list

def generate_one_dict(_dict, key):
    result_dict = {}
    result_dict["ITEM"] = key 
    result_dict["TTLS"] = _dict["TTLS"]
    result_dict["FIRST_SEEN"] = _dict["FIRST_SEEN"]
    result_dict["LAST_SEEN"] = _dict["LAST_SEEN"]
    result_dict["COUNT"] = _dict["COUNT"]
    return result_dict

def update(_coll, _dict, _doc, key, sus_coll, sus_set):
    index_dict = {} 
    number = 0
    for item in _doc:
        index_dict[item["ITEM"]] = number
        number = number + 1
    
    number = 0
    for item in _dict:
        if item in index_dict:
            number = number + 1
            index = index_dict[item]
            new_ttl_list = []
            for ttl in _dict[item]["TTLS"]:
                if ttl not in _doc[index]["TTLS"]:
                    new_ttl_list.append(ttl)
            if len(new_ttl_list) > 0:
                _coll.update({"_id":key, "ITEMS.ITEM":item}, \
                        {"$addToSet":{"ITEMS.$.TTLS":{"$each":new_ttl_list}}})
            if _dict[item]["FIRST_SEEN"] < _doc[index]["FIRST_SEEN"]:
                _coll.update({"_id":key, "ITEMS.ITEM":item}, \
                        {"$set":{"ITEMS.$.FIRST_SEEN":_dict[item]["FIRST_SEEN"]}})
            if _dict[item]["LAST_SEEN"] > _doc[index]["LAST_SEEN"]:
                _coll.update({"_id":key, "ITEMS.ITEM":item}, \
                        {"$set":{"ITEMS.$.LAST_SEEN":_dict[item]["LAST_SEEN"]}})
            _coll.update({"_id":key, "ITEMS.ITEM":item}, \
                        {"$set":{"ITEMS.$.COUNT":_dict[item]["COUNT"]+_doc[index]["COUNT"]}})
        else:
            new_dict = generate_one_dict(_dict[item], item) 
            new_dict["TTLS"] = list(new_dict["TTLS"])
            _coll.update({"_id":key}, {"$push":{"ITEMS":new_dict}})

    if (len(index_dict) + len(_dict) - number) > 200 :
        _doc = _coll.find_one({"_id":key}) 
        new_sus_dict = transform_doc(_doc["ITEMS"])
        insert_nor_sus(sus_coll, key, new_sus_dict)
        sus_set.add(key)
        _coll.remove({"_id":key})
        

def dump_dict(_coll, nor_coll, sus_coll, nor_set, sus_set, _dict): 
    for key1 in _dict:
        if key1 in sus_set:
            new_sus_dict = transform_dict(_dict[key1])
            sus_doc = sus_coll.find_one({"_id":key1})
            update_nor_sus(sus_coll, key1, new_sus_dict, sus_doc)
        elif is_in_set(nor_set, key1):
            new_nor_dict = transform_dict(_dict[key1])
            nor_doc = nor_coll.find_one({"_id":key1})
            update_nor_sus(nor_coll, key1, new_nor_dict, nor_doc)
        else:
            _doc = _coll.find_one({"_id":key1})
            if _doc == None:
                new_list = generate_list(_dict[key1])
                for item in new_list:
                    item["TTLS"]=list(item["TTLS"])
                _coll.insert({"_id":key1, "ITEMS":new_list})
            else:
                mongo_list = _doc["ITEMS"]
                update(_coll, _dict[key1], mongo_list, key1, sus_coll, sus_set) 
                
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
sus_domain_set = Set([])

nor_ip_set = Set([])
nor_domain_set = Set([])
init_nor_set("domain_whitelist.txt", nor_domain_set) 

root = "/dnscap-data1/dnslog/"

mongo_print(str(os.path.basename(__file__)))

for root, dirs, files in os.walk(root): 
    files.sort()
    for f in files:
        if not p.match(f):continue 
        this_date = datetime.strptime(f,"%Y-%m-%d").date() 
        if this_date > date(2013,9,14) and this_date < date(2013, 10, 1):
            file_list.append(f)

for f in file_list:
    init_sus_set(db.sus_ip, sus_ip_set) 
    init_sus_set(db.sus_domain, sus_domain_set) 

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
    dump_dict(db.domain, db.nor_domain, db.sus_domain, nor_domain_set, sus_domain_set, domain_dict)
    mongo_print((time.clock() - bar2) / len(domain_dict))  
    mongo_print(datetime.now())
    domain_dict.clear()


    bar3 = time.clock()
    dump_dict(db.ip, db.nor_ip, db.sus_ip, nor_ip_set, sus_ip_set, ip_dict) 
    mongo_print((time.clock() - bar3) / len(ip_dict))
    mongo_print(str(datetime.now()) + "\n")
    ip_dict.clear()


