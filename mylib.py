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

def init_domain_set(f, nor_domain_set):
    for line in open(f):
        nor_domain_set.add(line.strip('\n'))

def mongo_print(args):
    run_log_writer = open("run.log", "a")
    print str(args)
    run_log_writer.write(str(args)+"\n")
    run_log_writer.close()

def load_one_line(_dict, timestamp, subdomain, key, item, ttl, count):
    if key not in _dict:
        _dict[key] = {}
        _dict[key]["_id"] = key
        _dict[key]["ITEMS"] = [item]
        _dict[key]["SUBDOMAINS"] = [subdomain]
        _dict[key]["TTLS"] = [ttl]
        _dict[key]["COUNT"] = count
        _dict[key]["FIRST_SEEN"] = timestamp
        _dict[key]["LAST_SEEN"] = timestamp
        return

    if item not in _dict[key]["ITEMS"]:
        _dict[key]["ITEMS"].append(item)
    if subdomain not in _dict[key]["SUBDOMAINS"]:
        _dict[key]["SUBDOMAINS"].append(subdomain)
    if ttl not in _dict[key]["TTLS"]:
        _dict[key]["TTLS"].append(ttl)
    if timestamp < _dict[key]["FIRST_SEEN"]: _dict[key]["FIRST_SEEN"] = timestamp
    if timestamp > _dict[key]["LAST_SEEN"]: _dict[key]["LAST_SEEN"] = timestamp
    _dict[key]["COUNT"] = _dict[key]["COUNT"] + count

