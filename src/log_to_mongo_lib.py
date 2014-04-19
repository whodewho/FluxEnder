from datetime import datetime
import pandas as pd
import csv
from urlparse import urlparse

import tldextract


def init_set(coll, _set):
    cursor = coll.find({}, {"_id": 1})
    for item in cursor:
        _set.add(item["_id"])


def init_domain_set(f, domain_set):
    for line in open(f):
        ext = tldextract.extract(line.strip())
        domain_set.add(".".join(ext[1:]))


def init_sus_domain_set():
    sus_domain_set = set([])
    init_domain_set("../resources/hosts_badzeus.txt", sus_domain_set)
    init_domain_set("../resources/hosts_spyeye.txt", sus_domain_set)
    init_domain_set("../resources/hosts_palevo.txt", sus_domain_set)
    init_domain_set("../resources/hosts_feodo.txt", sus_domain_set)
    init_domain_set("../resources/hosts_cybercrime.txt", sus_domain_set)
    init_domain_set("../resources/hosts_malwaredomains.txt", sus_domain_set)
    init_domain_set("../resources/hosts_malwaredomainlist.txt", sus_domain_set)
    init_from_phishtank(sus_domain_set)
    init_domain_set("../resources/hosts_hphosts.txt", sus_domain_set)
    return sus_domain_set


def init_outlier_domain_set():
    outlier_domain_set = set([])
    init_domain_set("../resources/domain_whitelist.txt", outlier_domain_set)
    init_domain_set("../resources/cdn.txt", outlier_domain_set)
    return outlier_domain_set


def init_from_phishtank(domain_set):
    with open('../resources/hosts_phishtank.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            ext = tldextract.extract(urlparse(row[1]).netloc)
            # print ".".join(ext[1:])
            domain_set.add(".".join(ext[1:]))


def init_from_alexa(begin=1, end=10000):
    df = pd.read_csv('../resources/top-1m.csv', names=['rank', 'domain'], header=None, encoding='utf-8')
    return set(list(df[(df['rank'] <= end) & (df['rank'] >= begin)]['domain']))


def mongo_print(args):
    run_log_writer = open("run.log", "a")
    print str(args)
    run_log_writer.write(str(args) + "\n")
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
    if timestamp < _dict[key]["FIRST_SEEN"]:
        _dict[key]["FIRST_SEEN"] = timestamp
    if timestamp > _dict[key]["LAST_SEEN"]:
        _dict[key]["LAST_SEEN"] = timestamp
    _dict[key]["COUNT"] = _dict[key]["COUNT"] + count


# --------------------------------------------------------------------------------------

def dump_domain_dict(coll, nor_coll, sus_coll, spe_coll,
                     nor_set, sus_set,
                     _dict,
                     cache, nor_cache, sus_cache, spe_cache,
                     nor_ip_set, sus_ip_set):
    for key in _dict:
        if key in sus_set:
            sus_doc = sus_coll.find_one({"_id": key})
            sus_ip_set |= set(_dict[key]["ITEMS"])
            update_nor_sus(key, _dict[key], sus_doc, sus_cache)
        elif key in nor_set:
            nor_doc = nor_coll.find_one({"_id": key})
            nor_ip_set |= set(_dict[key]["ITEMS"])
            update_nor_sus(key, _dict[key], nor_doc, nor_cache)
        else:
            doc = coll.find_one({"_id": key})
            update(_dict[key], doc, key, cache, spe_cache)

    clear_cache(coll, cache)
    clear_cache(nor_coll, nor_cache)
    clear_cache(sus_coll, sus_cache)
    clear_cache(spe_coll, spe_cache)


def dump_ip_dict(coll, nor_coll, sus_coll, spe_coll,
                 nor_set, sus_set,
                 _dict,
                 cache, nor_cache, sus_cache, spe_cache):
    for key in _dict:
        if key in sus_set:
            sus_doc = sus_coll.find_one({"_id": key})
            update_nor_sus(key, _dict[key], sus_doc, sus_cache)
        elif key in nor_set:
            nor_doc = nor_coll.find_one({"_id": key})
            update_nor_sus(key, _dict[key], nor_doc, nor_cache)
        else:
            doc = coll.find_one({"_id": key})
            update(_dict[key], doc, key, cache, spe_cache)

    clear_cache(coll, cache)
    clear_cache(nor_coll, nor_cache)
    clear_cache(sus_coll, sus_cache)
    clear_cache(spe_coll, spe_cache)


def update_nor_sus(key, _dict, doc, nor_sus_cache):
    if doc is None:
        nor_sus_cache["INSERT"][key] = _dict
        #coll.insert("_id":key, _dict)
    else:
        update_dict = {}

        new_item_list = list(set(_dict["ITEMS"]) - set(doc["ITEMS"]))
        if len(new_item_list) > 0:
            update_dict["$push"] = {}
            update_dict["$push"]["ITEMS"] = {"$each": new_item_list}

        new_subdomain_list = list(set(_dict["SUBDOMAINS"]) - set(doc["SUBDOMAINS"]))
        if len(new_subdomain_list) > 0:
            if "$push" not in update_dict:
                update_dict["$push"] = {}
            update_dict["$push"]["SUBDOMAINS"] = {"$each": new_subdomain_list}

        new_ttl_list = list(set(_dict["TTLS"]) - set(doc["TTLS"]))
        if len(new_ttl_list) > 0:
            if "$push" not in update_dict:
                update_dict["$push"] = {}
            update_dict["$push"]["TTLS"] = {"$each": new_ttl_list}

        update_dict["$set"] = {}
        if _dict["FIRST_SEEN"] < doc["FIRST_SEEN"]:
            update_dict["$set"]["FIRST_SEEN"] = _dict["FIRST_SEEN"]
        if _dict["LAST_SEEN"] > doc["LAST_SEEN"]:
            update_dict["$set"]["LAST_SEEN"] = _dict["LAST_SEEN"]
        update_dict["$set"]["COUNT"] = doc["COUNT"] + _dict["COUNT"]

        nor_sus_cache["UPDATE"][key] = update_dict
        #coll.update({"_id":key}, update_dict)


def update(_dict, doc, key, cache, spe_cache):
    spe_threshold = 30
    if doc is None:
        if len(_dict["ITEMS"]) > spe_threshold:
            spe_cache["INSERT"][key] = _dict
        else:
            cache["INSERT"][key] = _dict
    else:
        update_dict = {}
        di_set = set(_dict["ITEMS"])
        do_set = set(doc["ITEMS"])
        if len(di_set | do_set) > spe_threshold:
            update_dict["ITEMS"] = list(set(_dict["ITEMS"]) | set(doc["ITEMS"]))
            update_dict["SUBDOMAINS"] = list(set(_dict["SUBDOMAINS"]) | set(doc["SUBDOMAINS"]))
            update_dict["TTLS"] = list(set(_dict["TTLS"]) | set(doc["TTLS"]))
            update_dict["FIRST_SEEN"] = min(_dict["FIRST_SEEN"], doc["FIRST_SEEN"])
            update_dict["LAST_SEEN"] = max(_dict["FIRST_SEEN"], doc["FIRST_SEEN"])
            update_dict["COUNT"] = _dict["COUNT"] + doc["COUNT"]
            update_dict["_id"] = key
            spe_cache["INSERT"][key] = update_dict
            cache["DELETE"].add(key)
        else:
            new_item_list = list(di_set - do_set)
            if len(new_item_list) > 0:
                update_dict["$push"] = {}
                update_dict["$push"]["ITEMS"] = {"$each": new_item_list}

            new_subdomain_list = list(set(_dict["SUBDOMAINS"]) - set(doc["SUBDOMAINS"]))
            if len(new_subdomain_list) > 0:
                if "$push" not in update_dict: update_dict["$push"] = {}
                update_dict["$push"]["SUBDOMAINS"] = {"$each": new_subdomain_list}

            new_ttl_list = list(set(_dict["TTLS"]) - set(doc["TTLS"]))
            if len(new_ttl_list) > 0:
                if "$push" not in update_dict:
                    update_dict["$push"] = {}
                update_dict["$push"]["TTLS"] = {"$each": new_ttl_list}

            update_dict["$set"] = {}
            if _dict["FIRST_SEEN"] < doc["FIRST_SEEN"]:
                update_dict["$set"]["FIRST_SEEN"] = _dict["FIRST_SEEN"]
            if _dict["LAST_SEEN"] > doc["LAST_SEEN"]:
                update_dict["$set"]["LAST_SEEN"] = _dict["LAST_SEEN"]
            update_dict["$set"]["COUNT"] = doc["COUNT"] + _dict["COUNT"]

            cache["UPDATE"][key] = update_dict


def init_cache():
    cache = dict()
    cache["UPDATE"] = {}
    cache["INSERT"] = {}
    cache["DELETE"] = set([])
    return cache


def clear_cache(coll, cache):
    mongo_print(str(datetime.now()) + " begin clear")

    for item in cache["INSERT"]:
        coll.insert(cache["INSERT"][item])
    mongo_print(str(datetime.now()) + " " + str(len(cache["INSERT"])) + " inserted")
    cache["INSERT"].clear()

    for item in cache["UPDATE"]:
        coll.update({"_id": item}, cache["UPDATE"][item])
    mongo_print(str(datetime.now()) + " " + str(len(cache["UPDATE"])) + " updated")
    cache["UPDATE"].clear()

    for item in cache["DELETE"]:
        coll.remove({"_id": item})
    mongo_print(str(datetime.now()) + " " + str(len(cache["DELETE"])) + " deleted")
    cache["DELETE"].clear()

