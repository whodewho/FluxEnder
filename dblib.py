from mylib import *

def update_nor_domain(coll, key, _dict, doc, nor_sus_cache, nor_ip_set):
    if doc == None:
        _dict["_id"] = key
        nor_sus_cache["INSERT"][key] = _dict
        for ip in _dict["ITEMS"]:nor_ip_set.add(ip)
    else:
        update_dict = {}
        new_item_list = []
        doc_item_set = Set([])
        tmp_flag = False
        for item in doc["ITEMS"]:
            doc_item_set.add(item)
        for item in _dict["ITEMS"]:
            if item not in doc_item_set:
                tmp_flag = True
                new_item_list.append(item)
                nor_ip_set.add(item)
        if tmp_flag:
            update_dict["$push"] = {}
            update_dict["$push"]["ITEMS"] = {"$each":new_item_list}

        new_ttl_list = []
        for ttl in _dict["TTLS"]:
            if ttl not in doc["TTLS"]:
                new_ttl_list.append(ttl)
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

        nor_sus_cache["UPDATE"][key] = update_dict
        #coll.update({"_id":key}, update_dict)

def update_nor_sus(coll, key, _dict, doc, nor_sus_cache):
    if doc == None:
        _dict["_id"] = key
        nor_sus_cache["INSERT"][key] = _dict
        #coll.insert("_id":key, _dict)
    else:
        update_dict = {}
        new_item_list = []
        doc_item_set = Set([])
        tmp_flag = False
        for item in doc["ITEMS"]:
            doc_item_set.add(item)
        for item in _dict["ITEMS"]:
            if item not in doc_item_set:
                tmp_flag = True
                new_item_list.append(item)
        if tmp_flag:
            update_dict["$push"] = {}
            update_dict["$push"]["ITEMS"] = {"$each":new_item_list}

        new_ttl_list = []
        for ttl in _dict["TTLS"]:
            if ttl not in doc["TTLS"]:
                new_ttl_list.append(ttl)
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

        nor_sus_cache["UPDATE"][key] = update_dict
        #coll.update({"_id":key}, update_dict)


def update(coll, _dict, doc, key, sus_coll, sus_set, cache, sus_cache):
    index_dict = {} 
    index = 0
    for item in doc:
        index_dict[item["ITEM"]] = index
        index = index + 1

    number1 = 0
    number2 = 0
    update_dict = {}
    for item in _dict:
        if item in index_dict:
            number1 = number1 + 1
            index = index_dict[item]
            new_ttl_list = []
            for ttl in _dict[item]["TTLS"]:
                if ttl not in doc[index]["TTLS"]:
                    new_ttl_list.append(ttl)
            if len(new_ttl_list) > 0:
                if "$push" not in update_dict:
                    update_dict["$push"] = {}
                update_dict["$push"]["ITEMS."+str(index)+".TTLS"] = {"$each":new_ttl_list}
            
            update_dict["$set"] = {}
            if _dict[item]["FIRST_SEEN"] < doc[index]["FIRST_SEEN"]:
                update_dict["$set"]["ITEMS."+str(index)+".FIRST_SEEN"]=_dict[item]["FIRST_SEEN"]
            if _dict[item]["LAST_SEEN"] > doc[index]["LAST_SEEN"]:
                update_dict["$set"]["ITEMS."+str(index)+".LAST_SEEN"]=_dict[item]["LAST_SEEN"]
            update_dict["$set"]["ITEMS."+str(index)+".COUNT"]=_dict[item]["COUNT"]+doc[index]["COUNT"]

        else:
            number2 = number2 + 1
            new_dict = generate_one_dict(_dict[item], item) 
            if "$push" not in update_dict:
                update_dict["$push"] = {}
            if "ITEMS" not in update_dict["$push"]:
                update_dict["$push"]["ITEMS"] = {"$each":[]}
            update_dict["$push"]["ITEMS"]["$each"].append(new_dict)

    cache["UPDATE"][key] = update_dict
    tmp = len(index_dict) + len(_dict) - number1
    if (tmp) > 40 :
        #print len(index_dict), len(_dict), number1, number2, tmp, key
        #doc = coll.find_one({"_id":key}) 
        from_doc = transform_doc(doc)
        from_dict= transform_dict(_dict)
        new_sus_dict = merge_doc_dict(from_doc, from_dict)
        new_sus_dict["_id"] = key
        sus_cache["INSERT"][key] = new_sus_dict
        del cache["UPDATE"][key]
        cache["DELETE"].add(key)
        sus_set.add(key)

def init_cache():
    cache = {}
    cache["UPDATE"]={}
    cache["INSERT"]={}
    cache["DELETE"]=Set([])
    return cache

def clear_cache(coll, cache, flag):
    mongo_print(str(datetime.now()) + " begin clear")

    for item in cache["INSERT"]:
        coll.insert(cache["INSERT"][item])
    mongo_print(str(datetime.now()) +" "+ str(len(cache["INSERT"])) + " inserted")
    cache["INSERT"].clear()

    if flag:
        for item in cache["UPDATE"]:
            new_items = None 
            if "$push" in cache["UPDATE"][item] and "ITEMS" in cache["UPDATE"][item]["$push"]:
                new_items = cache["UPDATE"][item]["$push"]["ITEMS"]
                del cache["UPDATE"][item]["$push"]["ITEMS"]
                if len(cache["UPDATE"][item]["$push"]) == 0:
                    del cache["UPDATE"][item]["$push"]
            coll.update({"_id":item}, cache["UPDATE"][item])
            if new_items != None:
                coll.update({"_id":item}, {"$push":{"ITEMS":new_items}})
    else:
        for item in cache["UPDATE"]:
            coll.update({"_id":item}, cache["UPDATE"][item])

    mongo_print(str(datetime.now()) +" "+ str(len(cache["UPDATE"])) + " updated")
    cache["UPDATE"].clear()

    for item in cache["DELETE"]:
        coll.remove({"_id":item})
    mongo_print(str(datetime.now()) +" "+ str(len(cache["DELETE"])) + " deleted")
    cache["DELETE"].clear()


