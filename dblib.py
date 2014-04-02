from mylib import *
from datetime import datetime


def dump_domain_dict(coll, nor_coll, sus_coll, nor_set, sus_set,
                     _dict, cache, nor_cache, sus_cache, nor_ip_set):
    for key in _dict:
        if key in sus_set:
            sus_doc = sus_coll.find_one({"_id": key})
            update_nor_sus(key, _dict[key], sus_doc, sus_cache)
        elif key in nor_set:
            nor_doc = nor_coll.find_one({"_id": key})
            nor_ip_set = nor_ip_set | set(_dict[key]["ITEMS"])
            update_nor_sus( key, _dict[key], nor_doc, nor_cache)
        else:
            doc = coll.find_one({"_id": key})
            update(_dict[key], doc, key, sus_set, cache, sus_cache)
    
    clear_cache(coll, cache)
    clear_cache(nor_coll, nor_cache)
    clear_cache(sus_coll, sus_cache)


def dump_ip_dict(coll, nor_coll, sus_coll, nor_set, sus_set,
                 _dict, cache, nor_cache, sus_cache):
    for key in _dict:
        if key in sus_set:
            sus_doc = sus_coll.find_one({"_id": key})
            update_nor_sus(key, _dict[key], sus_doc, sus_cache)
        elif key in nor_set:
            nor_doc = nor_coll.find_one({"_id": key})
            update_nor_sus(key, _dict[key], nor_doc, nor_cache)
        else:
            doc = coll.find_one({"_id": key})
            update(_dict[key], doc, key, sus_set, cache, sus_cache)

    clear_cache(coll, cache)
    clear_cache(nor_coll, nor_cache)
    clear_cache(sus_coll, sus_cache)


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


def update(_dict, doc, key, sus_set, cache, sus_cache):
    sus_threshold = 20
    if doc is None:
        if len(_dict["ITEMS"]) > sus_threshold:
            sus_cache["INSERT"][key] = _dict
        else:
            cache["INSERT"][key] = _dict
    else:
        update_dict = {}
        di_set = set(_dict["ITEMS"])
        do_set = set(doc["ITEMS"])
        if len(di_set | do_set) > sus_threshold:
            update_dict["ITEMS"] = list(set(_dict["ITEMS"]) | set(doc["ITEMS"]))
            update_dict["SUBDOMAINS"] = list(set(_dict["SUBDOMAINS"]) | set(doc["SUBDOMAINS"]))
            update_dict["TTLS"] = list(set(_dict["TTLS"]) | set(doc["TTLS"]))
            update_dict["FIRST_SEEN"] = min(_dict["FIRST_SEEN"], doc["FIRST_SEEN"])
            update_dict["LAST_SEEN"] = max(_dict["FIRST_SEEN"], doc["FIRST_SEEN"])
            update_dict["COUNT"] = _dict["COUNT"] + doc["COUNT"]
            update_dict["_id"] = key
            sus_cache["INSERT"][key] = update_dict
            cache["DELETE"].add(key)
            sus_set.add(key)
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
                update_dict["$push"]["TTLS"] = {"$each":new_ttl_list}

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


