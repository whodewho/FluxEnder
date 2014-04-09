import os
import re
import tldextract
from mylib import init_domain_set
from datetime import datetime


root = "/home/kai/Workspace/graduation/dnscap-data1/dnslog/"
p = re.compile("^\d{4}\-\d{2}\-\d{2}$")
file_list = []
domain_set4 = set([])
init_domain_set("hosts_badZeuS.txt", domain_set4)
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

        domain = (line_array[4][:len(line_array[4])-1]).lower()
        ext = tldextract.extract(domain)
        if ext.domain == "":
            domain = ext.suffix
        else:
            domain = ".".join(ext[1:])

        if domain in domain_set4:
            print file_name, line