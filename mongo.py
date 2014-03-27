from mylib import *
from dblib import *
      
def dump_domain_dict(coll, nor_coll, sus_coll, nor_set, sus_set, \
        _dict, cache, nor_cache, sus_cache): 
    for key in _dict:
        if key in sus_set:
            new_sus_dict = transform_dict(_dict[key])
            sus_doc = sus_coll.find_one({"_id":key})
            update_nor_sus(sus_coll, key, new_sus_dict, sus_doc, sus_cache)
        elif is_in_nor_domain_set(nor_set, key):
            new_nor_dict = transform_dict(_dict[key])
            nor_doc = nor_coll.find_one({"_id":key})
            update_nor_domain(nor_coll, key, new_nor_dict, nor_doc, nor_cache, nor_ip_set)
        else:
            _doc = coll.find_one({"_id":key})
            if _doc == None:
                new_list = generate_list(_dict[key])
                cache["INSERT"][key] = {"_id":key, "ITEMS":new_list}
            else:
                item_list = _doc["ITEMS"]
                update(coll, _dict[key], item_list, key, sus_coll, sus_set, cache, sus_cache) 
    
    clear_cache(coll, cache, True)
    clear_cache(nor_coll, nor_cache, False)
    clear_cache(sus_coll, sus_cache, False)
   
def dump_ip_dict(coll, nor_coll, sus_coll, nor_set, sus_set, \
        _dict, cache, nor_cache, sus_cache): 
    for key in _dict:
        if key in sus_set:
            new_sus_dict = transform_dict(_dict[key])
            sus_doc = sus_coll.find_one({"_id":key})
            update_nor_sus(sus_coll, key, new_sus_dict, sus_doc, sus_cache)
        elif key in nor_set:
            new_nor_dict = transform_dict(_dict[key])
            nor_doc = nor_coll.find_one({"_id":key})
            update_nor_sus(nor_coll, key, new_nor_dict, nor_doc, nor_cache)
        else:
            _doc = coll.find_one({"_id":key})
            if _doc == None:
                new_list = generate_list(_dict[key])
                cache["INSERT"][key] = {"_id":key, "ITEMS":new_list}
            else:
                item_list = _doc["ITEMS"]
                update(coll, _dict[key], item_list, key, sus_coll, sus_set, cache, sus_cache) 

    clear_cache(coll, cache, True)
    clear_cache(nor_coll, nor_cache, False)
    clear_cache(sus_coll, sus_cache, False)

 
#--------------Main-----------------#
root = "/dnscap-data1/dnslog/"
p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
start_date_str = "140219"
start_date = datetime.strptime(start_date_str, "%y%m%d")
end_date = start_date + timedelta(days = 7)

client = MongoClient()
db = client["p"+start_date_str]
file_list = []

ip_dict = {}
domain_dict = {}
sus_ip_set = Set([])
sus_domain_set = Set([])
nor_ip_set = Set([]) 
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
        if this_date >= start_date.date() and this_date < end_date.date():
            file_list.append(f)

for f in file_list:
    init_set(db.sus_ip, sus_ip_set) 
    init_set(db.sus_domain, sus_domain_set) 
    init_set(db.nor_ip, nor_ip_set)

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
    dump_domain_dict(db.domain, db.nor_domain, db.sus_domain, nor_domain_set, sus_domain_set, domain_dict, \
            domain_cache, nor_domain_cache, sus_domain_cache)
    mongo_print((time.clock() - bar2) / len(domain_dict))  
    mongo_print(datetime.now())
    domain_dict.clear()

    bar3 = time.clock()
    dump_ip_dict(db.ip, db.nor_ip, db.sus_ip, nor_ip_set, sus_ip_set, ip_dict, \
            ip_cache, nor_ip_cache, sus_ip_cache) 
    mongo_print((time.clock() - bar3) / len(ip_dict))
    mongo_print(str(datetime.now()) + "\n")
    ip_dict.clear()


