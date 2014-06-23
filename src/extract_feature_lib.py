from datetime import datetime, timedelta
import collections
import math
import tldextract
from __init__ import *


def get_tail(domain):
    tmp_arr = domain.split(".")
    tmp_len = len(tmp_arr)
    tmp_tail = tmp_arr[tmp_len-2] + "." + tmp_arr[tmp_len-1]
    return tmp_tail

#-----------------------------------------------------------------------


def entropy(s):
    p, lns = collections.Counter(s), float(len(s))
    if lns <= 1:
        return 0
    return sum(- count/lns * math.log(count/lns, 2) for count in p.values())/(math.log(lns, 2))


def get_ip_stability(_dict):
    if len(_dict) <= 1:
        return 0
    whole_list = []
    for item in _dict:
        try:
            whole_list.extend([get_prefix16(ip) for ip in list(_dict[item])])
        except IndexError:
            print item
            print _dict[item]

    whole_counter = collections.Counter(whole_list)
    tmp = 0
    for item in _dict:
        for sub_item in _dict[item]:
            if whole_counter[get_prefix16(sub_item)] - 1 == 0:
                tmp += 1
                break

    return float(tmp)/len(_dict)


def get_domain_stability(_dict):
    if len(_dict) <= 1:
        return 0
    whole_list = []
    for item in _dict:
        try:
            whole_list.extend(list(_dict[item]))
        except IndexError:
            print item
            print _dict[item]

    whole_counter = collections.Counter(whole_list)
    tmp = 0
    for item in _dict:
        for sub_item in _dict[item]:
            if whole_counter[sub_item] - 1 == 0:
                tmp += 1
                break

    return float(tmp)/len(_dict)

#------------------------------------------------------------------------------------------------


def get_prefix16(ip):
    tmp_array = ip.split(".")
    return tmp_array[0] + "." + tmp_array[1]


def ip_diversity(ips):
    prefix16_list = [get_prefix16(x) for x in ips]
    return round(entropy(prefix16_list), 5)


def relative_domain(ips, db_name):
    ip_domain_dict = {}
    domain_2tld_set = set([])
    domain_set = set([])
    for ip in ips:
        for index in range(4, 8):
            res = client[db_name][coll_name_list[index]].find_one({"_id": ip})
            if res:
                ip_domain_dict[ip] = res["ITEMS"]
                domain_2tld_set |= set(res["ITEMS"])
                domain_set |= set([tldextract.extract(x).domain for x in res["ITEMS"]])
                break

        if ip not in ip_domain_dict:
            print "bingo!"

    result = [len(domain_2tld_set), len(domain_set)/float(len(domain_2tld_set)), get_domain_stability(ip_domain_dict)]
    return [round(x, 5) for x in result]


def growth(domain, ips, subdomains, db_name):
    result = [1] * 6
    prefix16 = set([get_prefix16(x) for x in ips])
    pre_prefix16 = set([])
    pre_subdomains = set([])

    for i in range(1, 9):
        pre_db_name = "p" + (datetime.strptime(db_name[1:], "%y%m%d") - timedelta(days=i)).strftime("%y%m%d")
        if pre_db_name not in client.database_names():
            if i in [2, 3]:
                result[1] = result[2] = len(prefix16 - pre_prefix16)/float(len(prefix16))
                result[4] = result[5] = len(set(subdomains) - pre_subdomains)/float(len(subdomains))
            elif i in [5, 6, 7]:
                result[2] = len(prefix16 - pre_prefix16)/float(len(prefix16))
                result[5] = len(set(subdomains) - pre_subdomains)/float(len(subdomains))
            break
        for j in range(0, 4):
            res = client[pre_db_name][coll_name_list[j]].find_one({"_id": domain})
            if res:
                pre_prefix16 |= set([get_prefix16(x) for x in res["ITEMS"]])
                pre_subdomains |= set(res["SUBDOMAINS"])

        if i == 1:
            result[0] = result[1] = result[2] = len(prefix16 - pre_prefix16)/float(len(prefix16))
            result[3] = result[4] = result[5] = len(set(subdomains) - pre_subdomains)/float(len(subdomains))
        elif i == 4:
            result[1] = result[2] = len(prefix16 - pre_prefix16)/float(len(prefix16))
            result[4] = result[5] = len(set(subdomains) - pre_subdomains)/float(len(subdomains))
        elif i == 8:
            result[2] = len(prefix16 - pre_prefix16)/float(len(prefix16))
            result[5] = len(set(subdomains) - pre_subdomains)/float(len(subdomains))
    return [round(x, 5) for x in result]


def ip_info(ips, db_name):
    ip_statistic_dict = {}
    for ip in ips:
        for i in range(4, 8):
            res = client[db_name][coll_name_list[i]+"_matrix"].find_one({"_id": ip})
            if res:
                ip_statistic_dict[ip] = res
                break

    if len(ip_statistic_dict) == 0:
        return [-1, -1, -1]

    ip_ips_list = [ip_statistic_dict[x]['ips'] for x in ip_statistic_dict]
    ip_dga_list = [ip_statistic_dict[x]['dga'] for x in ip_statistic_dict]
    return [min(ip_ips_list), max(ip_ips_list), max(ip_dga_list)]


def subdomain_diversity(subdomains):
    len_list = [len(x) for x in subdomains]
    result = [len(subdomains), entropy(len_list)]
    return [round(x, 5) for x in result]
#------------------------------------------------------------------------------------------------


def domain_diversity(subdomains, domains):
    pre_list = subdomains
    ext_list = [tldextract.extract(domain) for domain in domains]
    mid_list = [ext.domain for ext in ext_list]
    suf_list = [ext.suffix for ext in ext_list]

    pre_len_list = [len(pre) for pre in pre_list]
    mid_len_list = [len(mid) for mid in mid_list]
    suf_len_list = [len(suf) for suf in suf_list]
    
    result = [entropy(pre_len_list), entropy(mid_len_list), entropy(suf_len_list)]
    return [round(x, 5) for x in result]


def ip_pool_stability(domains, db_name):
    domain_ip_dict = {}
    for domain in domains:
        for index in range(0, 4):
            res = client[db_name][coll_name_list[index]].find_one({"_id": domain})
            if res:
                domain_ip_dict[domain] = set(res["ITEMS"])
                break
        if domain not in domain_ip_dict:
            print "bingo!"

    return round(get_ip_stability(domain_ip_dict), 5)

