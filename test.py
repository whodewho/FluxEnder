__author__ = 'kai'
from mylib import *


sus_domain_set = set([])
init_domain_set("hosts_stdZeuS.txt", sus_domain_set)
print len(sus_domain_set)
init_domain_set("hosts_hphosts.txt", sus_domain_set)
print len(sus_domain_set)
init_domain_set("hosts_malwaredomainlist.txt", sus_domain_set)
print len(sus_domain_set)
init_domain_set("hosts_malwaredomains.txt", sus_domain_set)
print len(sus_domain_set)
init_from_phishtank(sus_domain_set)
print len(sus_domain_set)

