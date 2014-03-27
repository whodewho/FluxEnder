from sets import Set
import pymongo
from pymongo import MongoClient
from datetime import date, time, datetime, timedelta
from sets import Set
import re
import sys
import os
import json
import time
import tldextract

def init_set(coll, _set):
    cursor = coll.find({},{"_id":1})
    for item in cursor:
        _set.add(item["_id"])

def is_in_nor_domain_set(nor_set, key):
    key_array = key.split('.')
    l = len(key_array)
    if l < 2: return False
    key = key_array[l-2] + "." + key_array[l-1] 
    if key in nor_set: return True
    return False

def init_nor_domain_set(f, nor_domain_set):
    for line in open(f):
        nor_domain_set.add(line.strip('\n'))

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
        _dict[key1][key2]["TTLS"] = [int(_list[7])]
        _dict[key1][key2]["FIRST_SEEN"] = float(_list[0])
        _dict[key1][key2]["LAST_SEEN"] = float(_list[0])
        _dict[key1][key2]["COUNT"] = int(_list[8])
    else:
        if int(_list[7]) not in _dict[key1][key2]["TTLS"]:
            _dict[key1][key2]["TTLS"].append(int(_list[7]))
        if _dict[key1][key2]["FIRST_SEEN"] > float(_list[0]): _dict[key1][key2]["FIRST_SEEN"] = float(_list[0])
        if _dict[key1][key2]["LAST_SEEN"] < float(_list[0]): _dict[key1][key2]["LAST_SEEN"] = float(_list[0])
        _dict[key1][key2]["COUNT"] = _dict[key1][key2]["COUNT"] + int(_list[8])

#_dict {'ip':{}} or {'domain':{}}
def transform_dict(_dict):
    result_dict = {}
    result_dict["ITEMS"] = []
    result_dict["TTLS"] = []
    result_dict["FIRST_SEEN"] = 10000000000
    result_dict["LAST_SEEN"] = 0
    result_dict["COUNT"] = 0

    for key in _dict:
        result_dict["ITEMS"].append(key)
        for ttl in _dict[key]["TTLS"]:
            if ttl not in result_dict["TTLS"]:
                result_dict["TTLS"].append(ttl)
        if result_dict["FIRST_SEEN"] > _dict[key]["FIRST_SEEN"]: result_dict["FIRST_SEEN"] = _dict[key]["FIRST_SEEN"]
        if result_dict["LAST_SEEN"] < _dict[key]["LAST_SEEN"]: result_dict["LAST_SEEN"] = _dict[key]["LAST_SEEN"]
        result_dict["COUNT"] = result_dict["COUNT"] + _dict[key]["COUNT"]
    return result_dict

#doc {ITEM:ip/domain, FIRST_SEEN:xx, TTLS:[]}
def transform_doc(doc):
    result_dict = {}
    result_dict["ITEMS"] = []
    result_dict["TTLS"] = []
    result_dict["FIRST_SEEN"] = 10000000000
    result_dict["LAST_SEEN"] = 0
    result_dict["COUNT"] = 0

    for item in doc:
        result_dict["ITEMS"].append(item["ITEM"])
        for ttl in item["TTLS"]:
            if ttl not in result_dict["TTLS"]:
                result_dict["TTLS"].append(ttl)
        if result_dict["FIRST_SEEN"] > item["FIRST_SEEN"]: result_dict["FIRST_SEEN"] = item["FIRST_SEEN"]
        if result_dict["LAST_SEEN"] < item["LAST_SEEN"]: result_dict["LAST_SEEN"] = item["LAST_SEEN"]
        result_dict["COUNT"] = result_dict["COUNT"] + item["COUNT"]
    return result_dict

def merge_doc_dict(from_doc, from_dict):
    result_dict = {}
    result_dict["ITEMS"] = list(set(from_doc["ITEMS"] + from_dict["ITEMS"])) 
    result_dict["TTLS"] = list(set(from_doc["TTLS"] + from_dict["TTLS"])) 
    result_dict["FIRST_SEEN"] = from_doc["FIRST_SEEN"] if from_doc["FIRST_SEEN"] < from_dict["FIRST_SEEN"] else from_dict["FIRST_SEEN"] 
    result_dict["LAST_SEEN"] = from_doc["LAST_SEEN"] if from_doc["LAST_SEEN"] > from_dict["LAST_SEEN"] else from_dict["LAST_SEEN"] 
    result_dict["COUNT"] = from_doc["COUNT"] + from_dict["COUNT"] 
    #print result_dict
    return result_dict

#_dict domain/ip {ip/domain {},{},{}}, second level
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

#_dict {}, third level
def generate_one_dict(_dict, key):
    result_dict = {}
    result_dict["ITEM"] = key 
    result_dict["TTLS"] = _dict["TTLS"]
    result_dict["FIRST_SEEN"] = _dict["FIRST_SEEN"]
    result_dict["LAST_SEEN"] = _dict["LAST_SEEN"]
    result_dict["COUNT"] = _dict["COUNT"]
    return result_dict


