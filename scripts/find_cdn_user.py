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

# cdn_domain_set = init_from_alexa(1, 50)
cdn_domain_set = set([])
init_domain_set("../resources/cdn.txt", cdn_domain_set)
init_domain_set("../resources/domain_whitelist.txt", cdn_domain_set)


cdn_ip_set = set([])
new_cdn_domain = set([])
print cdn_domain_set

for f in file_list:
    for line in open(os.path.join(dns_log_path, f)):
        line_array = line.split("||")

        if len(line_array) != 9 or line_array[5] != 'A':
            continue

        domain = (line_array[4][:len(line_array[4]) - 1]).lower()
        ext = tldextract.extract(domain)
        if ext.domain == "":
            domain = ext.suffix
        else:
            domain = ".".join(ext[1:])

        ip = line_array[6]
        if domain in cdn_domain_set:
            cdn_ip_set.add(ip)
        elif ip in cdn_ip_set:
            new_cdn_domain.add(domain)


with open("../resources/cdn_new.txt", "a") as writer:
    for domain in new_cdn_domain:
        writer.write(domain + "\n")
with open("../resources/cdn_ip.txt", "w") as writer:
    for ip in cdn_ip_set:
        writer.write(ip + "\n")

