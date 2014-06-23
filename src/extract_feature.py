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
    cursor = client[db_name][coll_name_list[index]].find(timeout=False)
    outlier = {"", "ntp.org", "isipp.com", "gccdn.net", "cdngc.net", "gstatic.com", "cloudfront.net"}
    cache = {}


    num = 1
    print index
    for row in cursor:
        if get_tail(row["_id"]) in outlier:
            continue

        # if index == 0 and np.random.randint(1, 100) != 7:
        #     continue
        if index == 0:
            if np.random.randint(1, 50) != 7:
                flag = False
                this_db_name = db_name
                for m in range(0, 5):
                    pre_db_name = "p" + (datetime.strptime(this_db_name[1:], "%y%m%d") - timedelta(days=1)).strftime("%y%m%d")
                    if client[pre_db_name][coll_name_list[index]+"_matrix"].find_one({"_id": row["_id"]}):
                        flag = True
                        break
                    this_db_name = pre_db_name
                if not flag:
                    continue

            num += 1
            print "one more", num, row["_id"]

        ip_count = len(row["ITEMS"])
        min_ttl = min(row["TTLS"])
        max_ttl = max(row["TTLS"])
        lifetime = int(row["LAST_SEEN"] - row["FIRST_SEEN"])/(60*60*24)
        p16_entropy = ip_diversity(row["ITEMS"])

        if index == 2 and (ip_count < 2 or min_ttl > 20000 or p16_entropy < 0.08):
            continue

        gro = growth(row["_id"], row["ITEMS"], row["SUBDOMAINS"], db_name)
        relative = relative_domain(row["ITEMS"], db_name)
        ipinfo = ip_info(row["ITEMS"], db_name)
        if ipinfo[0] == -1:
            print "no ip", row["_id"], index, db_name
            continue

        subdomain = subdomain_diversity(row["SUBDOMAINS"])

        cache[row["_id"]] = {"ip_count": ip_count, "p16_entropy": p16_entropy,
                             "relative": relative,
                             "subdomain": subdomain,
                             "growth": gro,
                             "ipinfo": ipinfo,
                             "ttl": [min_ttl, max_ttl, max_ttl - min_ttl], "lifetime": lifetime, "_id": row["_id"]}
        # client[db_name][coll_name_list[index]+"_matrix"].insert(tmp)
    clear_cache(index, cache)


def extract_ip_feature(index):
    model = init_dga()
    cursor = client[db_name][coll_name_list[index]].find(timeout=False)
    cache = {}
    print index
    for row in cursor:
        if not ip_p.match(str(row["_id"])):
            continue

        number = len(row["ITEMS"])
        min_ttl = min(row["TTLS"])
        max_ttl = max(row["TTLS"])
        lifetime = int(row["LAST_SEEN"] - row["FIRST_SEEN"])/(60*60*24)

        dd = domain_diversity(row["SUBDOMAINS"], row["ITEMS"])
        ips = ip_pool_stability(row["ITEMS"], db_name)
        tmp_counter = collections.Counter(evaluate_url_list(model, row["ITEMS"]))
        dga = round(tmp_counter['dga']/float(number), 3)

        cache[row["_id"]] = {"number": number, "dd": dd, "ips": ips, "dga": dga,
                             "ttl": [min_ttl, max_ttl], "lifetime": lifetime, "_id": row["_id"]}
        # client[db_name][coll_name_list[index]+"_matrix"].insert(tmp)
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
