import re
from pymongo import MongoClient

__author__ = 'kai'

coll_name_list = ['domain', 'nor_domain', 'sus_domain', 'spe_domain', 'ip', 'nor_ip', 'sus_ip', 'spe_ip']
domain_p = re.compile("^[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,6}$")
ip_p = re.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")
client = MongoClient()
day_gap = 1
db_name = ""
