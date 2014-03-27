from mylib import *
from dblib import *
      
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

def dump_dict(coll, nor_coll, sus_coll, nor_set_dict, sus_set, \
        _dict, cache, nor_cache, sus_cache, is_domain): 
    for key in _dict:
        if key in sus_set:
            new_sus_dict = transform_dict(_dict[key])
            sus_doc = sus_coll.find_one({"_id":key})
            update_nor_sus(sus_coll, key, new_sus_dict, sus_doc, sus_cache)
        elif is_domain and is_in_nor_domain_set(nor_set_dict, key):
            new_nor_dict = transform_dict(_dict[key])
            nor_doc = nor_coll.find_one({"_id":key})
            update_nor_sus(nor_coll, key, new_nor_dict, nor_doc, nor_cache)
        elif not is_domain and is_in_nor_ip_dict(nor_set_dict, key):
            new_nor_dict = transform_dict(_dict[key])
            nor_doc = nor_coll.find_one({"_id":key})
            update_nor_sus(nor_coll, key, new_nor_dict, nor_doc, nor_cache)
        else:
            _doc = coll.find_one({"_id":key})
            if _doc == None:
                new_list = generate_list(_dict[key])
                cache["INSERT"][key] = {"_id":key, "ITEMS":new_list}
                #coll.insert({"_id":key, "ITEMS":new_list})
            else:
                item_list = _doc["ITEMS"]
                update(coll, _dict[key], item_list, key, sus_coll, sus_set, cache, sus_cache) 
    
    clear_cache(coll, cache, True)
    clear_cache(nor_coll, nor_cache, False)
    clear_cache(sus_coll, sus_cache, False)
    
 
#--------------Main-----------------#
root = "/dnscap-data1/dnslog/"
p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
client = MongoClient()
db = client.passiveDNS
file_list = []

ip_dict = {}
domain_dict = {}
sus_ip_set = Set([])
sus_domain_set = Set([])
nor_ip_dict = {} 
init_nor_ip_dict("ip_whitelist.txt", nor_ip_dict)
nor_domain_set = Set([])
init_nor_domain_set("domain_whitelist.txt", nor_domain_set) 

domain_cache = init_cache()
ip_cache = init_cache()
sus_domain_cache = init_cache()
sus_ip_cache = init_cache()
nor_domain_cache = init_cache()
nor_ip_cache = init_cache()

mongo_print("\n\n\n\n\n"+str(os.path.basename(__file__)))

for root, dirs, files in os.walk(root): 
    files.sort()
    for f in files:
        if not p.match(f):continue 
        this_date = datetime.strptime(f,"%Y-%m-%d").date() 
        if this_date > date(2013,9,13) and this_date < date(2013, 10, 1):
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
        
    mongo_print([len(domain_dict), len(ip_dict), number])
    mongo_print((time.clock() - bar1) / number)     
    mongo_print(datetime.now())

    bar2 = time.clock()
    dump_dict(db.domain, db.nor_domain, db.sus_domain, nor_domain_set, sus_domain_set, domain_dict, \
            domain_cache, nor_domain_cache, sus_domain_cache, True)
    mongo_print((time.clock() - bar2) / len(domain_dict))  
    mongo_print(datetime.now())
    domain_dict.clear()

    bar3 = time.clock()
    dump_dict(db.ip, db.nor_ip, db.sus_ip, nor_ip_dict, sus_ip_set, ip_dict, \
            ip_cache, nor_ip_cache, sus_ip_cache, False) 
    mongo_print((time.clock() - bar3) / len(ip_dict))
    mongo_print(str(datetime.now()) + "\n")
    ip_dict.clear()


