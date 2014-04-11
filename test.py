__author__ = 'kai'
from mylib import *


def gro(last):
    now = len(sus_domain_set)
    print now - last
    return now

sus_domain_set = set([])
last = 0
init_domain_set("hosts_badZeuS.txt", sus_domain_set)
last = gro(last)
# init_domain_set("hosts_hphosts.txt", sus_domain_set)
# last = gro(last)
init_domain_set("hosts_malwaredomainlist.txt", sus_domain_set)
last = gro(last)
init_domain_set("hosts_malwaredomains.txt", sus_domain_set)
last = gro(last)
init_from_phishtank(sus_domain_set)
last = gro(last)
init_domain_set("hosts_CyberCrime.txt", sus_domain_set)
last = gro(last)




