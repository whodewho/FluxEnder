from extract_feature_lib import *
from sys import argv
from dga_model_eval import *
from __init__ import *


def clear_cache(index, cache):
    print "clear cache", index
    for tmp in cache:
        client[db_name][coll_name_list[index]+"_matrix"].insert(cache[tmp])


def extract_domain_feature(index):
    #cursor = client[db_name].domain.find({"_id":{"$in": ["emltrk.com", "weminemnc.com"]}})
    cursor = client[db_name][coll_name_list[index]].find()
    outlier = {"", "ntp.org", "isipp.com", "gccdn.net", "cdngc.net", "gstatic.com", "cloudfront.net"}
    cache = {}
    i = 0
    #print_domain_line(index)
    for row in cursor:
        try:
            if get_tail(row["_id"]) in outlier:
                continue

            number = len(row["ITEMS"])
            min_ttl = min(row["TTLS"])
            max_ttl = max(row["TTLS"])
            lifetime = int(row["LAST_SEEN"] - row["FIRST_SEEN"])/(60*60*24)
            idi = ip_diversity(row["ITEMS"])

            if index == 2 and (number < 2 or min_ttl > 1000 or idi < 0.1):
                continue
            gro = growth(row["_id"], row["ITEMS"], row["SUBDOMAINS"], db_name)
            dps = domain_pool_stability(row["ITEMS"], db_name)
            ipi = ip_info(row["ITEMS"], db_name)
            sdd = subdomain_diversity(row["SUBDOMAINS"])

            i += 1
            cache[row["_id"]] = {"number": number, "idi": idi, "dps": dps, "sdd": sdd, "gro": gro, "ipi": ipi,
                                 "ttl": [min_ttl, max_ttl], "lifetime": lifetime, "_id": row["_id"]}

            if i == 20:
                i = 0
                #print_domain_line(index)
                #time.sleep(2)
        except KeyboardInterrupt:
            if not handle_exception(db_name):
                break

    clear_cache(index, cache) 


def extract_ip_feature(index):
    model = init_dga()
    cursor = client[db_name][coll_name_list[index]].find()
    cache = {}
    i = 0
    #print_ip_line(index)
    for row in cursor:
        try:
            number = len(row["ITEMS"])

            min_ttl = min(row["TTLS"])
            max_ttl = max(row["TTLS"])
            lifetime = int(row["LAST_SEEN"] - row["FIRST_SEEN"])/(60*60*24)

            dd = domain_diversity(row["SUBDOMAINS"], row["ITEMS"])
            ips = ip_pool_stability(row["ITEMS"], db_name)
            tmp_counter = collections.Counter(evaluate_url_list(model, row["ITEMS"]))
            dga = round(tmp_counter['dga']/float(number), 3) 

            i += 1
            cache[row["_id"]] = {"number": number, "dd": dd, "ips": ips, "dga": dga,
                                 "ttl": [min_ttl, max_ttl], "lifetime": lifetime, "_id": row["_id"]}

            if i == 20:
                i = 0
                #print_ip_line(index)
                #time.sleep(2)
        except KeyboardInterrupt:
            if not handle_exception(db_name):
                break

    clear_cache(index,  cache) 


def main(index):
    #index = int(raw_input())
    if index < 4:
        extract_domain_feature(index)
    elif index < 8:
        extract_ip_feature(index)
    else:
        for i in range(4, 8):
            extract_ip_feature(i)
        for i in range(0, 4):
            extract_domain_feature(i)

if __name__ == '__main__':
    script, db_name = argv
    main(8)
