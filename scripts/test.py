__author__ = 'kai'
from pymongo import MongoClient
import tldextract

from src.log_to_mongo_lib import init_domain_set, init_from_alexa, init_from_phishtank


sus_domain_set = set([])


def a():
    hosts = ["hosts_badzeus.txt", "hosts_spyeye.txt", "hosts_palevo.txt", "hosts_feodo.txt",
             "hosts_cybercrime.txt", "hosts_malwaredomains.txt", "hosts_malwaredomainlist.txt",
             "hosts_hphosts.txt"]
    whole = set([])
    for h in hosts:
        print len(whole)
        tmp = set([])
        init_domain_set(h, tmp)
        whole = whole | tmp
        print len(tmp), len(whole), h

    print len(whole)
    tmp = set([])
    init_from_phishtank(tmp)
    whole = whole | tmp
    print len(tmp), len(whole), "hosts_phishtank.csv"


def a1():
    ips = client["p140404"]["nor_domain"].find_one({"_id": "jd.com"})
    num = 0
    domain = ""
    for ip in ips["ITEMS"]:
        t = client["p140404"]["nor_ip"].find_one({"_id": ip})
        if len(t["SUBDOMAINS"]) > num:
            num = len(t["SUBDOMAINS"])
            domain = t
    print domain
    print len(domain["SUBDOMAINS"])


def a2():
    domains = client["p140316"]["sus_ip"].find_one({"_id": "8.23.224.90"})
    for d in domains["ITEMS"]:
        t = client["p140316"]["sus_domain"].find_one({"_id": d})
        print t, "\n"


def a3():
    db_name = "p140403"
    res = client[db_name]["sus_ip"].find_one({"_id": "8.23.224.90"})
    if res:
        print client[db_name]["sus_ip_matrix"].find_one({"_id": "8.23.224.90"})
        print res
        for d in res["ITEMS"]:
            print d
            print client[db_name]["sus_domain"].find_one({"_id": d})
            print client[db_name]["domain"].find_one({"_id": d})
            print client[db_name]["nor_domain"].find_one({"_id": d})
            print client[db_name]["spe_domain"].find_one({"_id": d})
            print ""
        print "-------------------------"


def a4():
    ips = client["p140404"]["nor_domain"].find_one({"_id": "facebook.com"})
    for ip in ips["ITEMS"]:
        for i in range(4, 8):
            t = client["p140404"][coll_name_list[i]].find_one({"_id": ip})
            if t and "twitter.com" in t["ITEMS"] and "youtube.com" in t["ITEMS"]:
                print ip
                # print t
                print len(t["ITEMS"])
            elif t:
                print "wode"


def a5():
    ips = client["p140404"]["nor_domain"].find_one({"_id": "renren.com"})
    for ip in ips["ITEMS"]:
        for i in range(4, 8):
            t = client["p140404"][coll_name_list[i]].find_one({"_id": ip})
            if t:
                print ip
                # print t
                print len(t["ITEMS"])
                print t["SUBDOMAINS"]
                print t["ITEMS"]
            elif t:
                print "wode"


def a6():
    ips = client["p140404"]["nor_domain"].find_one({"_id": "taobao.com"})
    for ip in ips["ITEMS"]:
        for i in range(4, 8):
            t = client["p140404"][coll_name_list[i] + "_matrix"].find_one({"_id": ip})
            if t:
                print t["ips"]


def a8():
    set1 = init_from_alexa(3001, 8000)
    set2 = set([])
    init_domain_set("tmp.txt", set2)
    print set2 & set1

    set3 = set([])
    init_domain_set("domain_whitelist.txt", set3)
    print set3 & set1

    print set3 & set2


def a9():
    set1 = set([])
    init_domain_set("fp.txt", set1)
    for d in set1:
        print client["p140414"]["nor_domain"].find_one({"_id": d})
        print client["p140414"]["nor_domain_matrix"].find_one({"_id": d})


def a10():
    db_name = "p140414"
    cursor = client[db_name]["sus_domain"].find()
    for row in cursor:
        if client[db_name]["nor_domain"].find_one({"_id": row['_id']}):
            print row


def get_prefix16(ip):
    tmp_array = ip.split(".")
    return tmp_array[0] + "." + tmp_array[1]


def a11():
    cursor = client["p140414"]["nor_domain"].find_one({"_id": "yunpan.cn"})
    set1 = set([])
    for ip in cursor["ITEMS"]:
        set1.add(get_prefix16(ip))
    print list(set1)


def a12():
    set1 = set([])
    cursor = client["p140414"]["nor_domain"].find({}, {"_id": 1})
    for item in cursor:
        set1.add(item["_id"])
    cursor = client["p140414"]["sus_domain"].find({}, {"_id": 1})
    for item in cursor:
        set1.add(item["_id"])

    with open("/home/kai/Workspace/graduation/dnscap-data1/dnslog/fluxbustertest/2014-04-14", "w") as writer:
        for line in open("/home/kai/Workspace/graduation/dnscap-data1/dnslog/2014-04-14"):
            line_array = line.split("||")
            if len(line_array) != 9 or line_array[5] != 'A':
                continue
            domain = (line_array[4][:len(line_array[4]) - 1]).lower()
            ext = tldextract.extract(domain)
            if ext.domain == "":
                domain = ext.suffix
            else:
                domain = ".".join(ext[1:])

            if domain in set1:
                writer.write(line)


def a13():
    import os
    import re
    from datetime import datetime
    import tldextract
    import ConfigParser

    Config = ConfigParser.ConfigParser()
    Config.read("config.ini")
    root = Config.get("One", "dns_log_path")
    p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
    file_list = []
    tmp_set = set([])
    init_domain_set("../resources/hosts_badzeus.txt", tmp_set)
    start_datetime = datetime(2014, 2, 1)
    end_datetime = datetime(2014, 3, 1)

    for root, dirs, files in os.walk(root):
        files.sort()
        for f in files:
            if not p.match(f):
                continue
            this_date = datetime.strptime(f, "%Y-%m-%d").date()
            if start_datetime.date() <= this_date < end_datetime.date():
                file_list.append(f)

    for file_name in file_list:
        print file_name
        for line in open(root + file_name):
            line_array = line.split("||")

            if len(line_array) != 9 or line_array[5] != 'A':
                continue

            domain = (line_array[4][:len(line_array[4]) - 1]).lower()
            ext = tldextract.extract(domain)
            if ext.domain == "":
                domain = ext.suffix
            else:
                domain = ".".join(ext[1:])

            if domain in tmp_set:
                print file_name, line

client = MongoClient()
coll_name_list = ['domain', 'nor_domain', 'sus_domain', 'spe_domain', 'ip', 'nor_ip', 'sus_ip', 'spe_ip']

