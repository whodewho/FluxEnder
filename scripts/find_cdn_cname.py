from sys import argv
import re
import os
from src.log_to_mongo_lib import *

script, start_date_str, end_data_str = argv
dns_log_path = "/home/kai/Workspace/graduation/dnscap-data1/dnslog/"
p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
start_datetime = datetime.strptime(start_date_str, "%y%m%d")
end_datetime = datetime.strptime(end_data_str, "%y%m%d")

file_list = []
for root, dirs, files in os.walk(dns_log_path):
    files.sort()
    for f in files:
        if not p.match(f):
            continue
        this_date = datetime.strptime(f, "%Y-%m-%d").date()
        if start_datetime.date() <= this_date < end_datetime.date():
            file_list.append(f)
print file_list

cdn_domain_set = set([])
init_domain_set("../resources/cdn.txt", cdn_domain_set)

cname_set = set([])
for f in file_list:
    for line in open(os.path.join(dns_log_path, f)):
        line_array = line.split("||")
        if len(line_array) != 9:
            continue

        domain = (line_array[4][:len(line_array[4]) - 1]).lower()
        ext = tldextract.extract(domain)
        if ext.domain == "":
            domain = ext.suffix
        else:
            domain = ".".join(ext[1:])

        if domain in cdn_domain_set and line_array[5] == "CNAME":
            new_domain = (line_array[6][:len(line_array[6]) - 1]).lower()
            ext = tldextract.extract(new_domain)
            if ext.domain == "":
                new_domain = ext.suffix
            else:
                new_domain = ".".join(ext[1:])
            if new_domain not in cdn_domain_set:
                print domain, "->", new_domain
                print line
                cname_set.add(new_domain)

with open("../resources/cdn_cname.txt", "w") as writer:
    for domain in cname_set:
        writer.write(domain + "\n")