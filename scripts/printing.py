from src.extract_feature_lib import client, domain_p, ip_p, coll_name_list
import time
from datetime import datetime


def find_in_all():
    while True:
    #for line in open("domains"):
        #response = line.strip()
        print "input your query"
        response = raw_input().strip()
        if response == "q":
            break
        if domain_p.match(response):
            for db_name in client.database_names():
                if len(db_name) < 7:
                    continue
                for coll_name in coll_name_list[0:4]:
                    ext = client[db_name][coll_name].find_one({"_id": response})
                    if ext:
                        writer = open("output/" + response, "a")
                        print db_name, coll_name, response
                        writer.write(str(ext) + "\n")
                        writer.write(str(client[db_name][coll_name+"_matrix"].find_one({"_id": response})) + "\n")
                        writer.close()
                        break
        elif ip_p.match(response):
            for db_name in client.database_names():
                if len(db_name) < 7:
                    continue
                for coll_name in coll_name_list[4:8]:
                    ext = client[db_name][coll_name].find_one({"_id": response})
                    if ext:
                        writer = open("output/" + response, "a")
                        print db_name, coll_name, response
                        writer.write(str(ext) + "\n")
                        writer.write(str(client[db_name][coll_name+"_matrix"].find_one({"_id": response})) + "\n")
                        writer.close()
                        break


def find_in_one():
    db = client["p140301"]
    while True:
        print "input collection"
        index = int(raw_input())
        cursor = db[coll_name_list[index - 1]+"_matrix"].find()
        i = 0

        for item in cursor:
            try:
                if index <= 3:
                    print_domain_item(item)
                else:
                    print_ip_item(item)

                i += 1
                if i == 20:
                    i = 0
                    if index <= 3:
                        print_domain_line(index)
                    else:
                        print_ip_line(index)
                    time.sleep(2)
            except KeyboardInterrupt:
                if not handle_exception(db):
                    break


def view_domain():
    db_name = "p" + str(datetime(2014, 1, 26).strftime("%y%m%d"))
    for index in range(0, 4):
        print_domain_line(index)
        cursor = client[db_name][coll_name_list[index]+"_matrix"].find()
        i = 0
        for item in cursor:
            if item['number'] < 3 or item['idi'] < 0.1 or item['gro'][0] < 0.1 or item['ttl'][0] > 2000:
                 continue
            try:
                print_domain_item(item)
                i += 1
                if i == 20:
                    i = 0
                    print_domain_line(index)
                    time.sleep(2)
            except KeyboardInterrupt:
                if not handle_exception(db_name):
                    break


def view_ip():
    db_name = "p" + str(datetime(2014, 3, 16).strftime("%y%m%d"))
    print db_name
    for index in range(4, 8):
        cursor = client[db_name][coll_name_list[index]+"_matrix"].find()
        i = 0
        for item in cursor:
            #if item["number"] < 3:
            #    continue
            try:
                print_ip_item(item)
                i += 1
                if i == 20:
                    i = 0
                    print_ip_line(index)
                    time.sleep(2)
            except KeyboardInterrupt:
                if not handle_exception(db_name):
                    break

if __name__ == '__main__':
    #view_domain()
    #view_ip()
    find_in_all()


#-----------------------------------------------------------------------------
def handle_exception(db_name):
    print '\nPausing...  (Hit ENTER to conti, type q to exit.)'
    response = raw_input().strip()
    while response:
        if response == 'q':
            break
        if domain_p.match(response):
            for i in range(0, 4):
                ext = client[db_name][coll_name_list[i]].find_one({"_id": response})
                if ext:
                    print i+1
                    print ext
                    print client[db_name][coll_name_list[i]+"_matrix"].find_one({"_id": response})
                    break
                else:
                    print "not found", coll_name_list[i]
        elif ip_p.match(response):
            for i in range(4, 8):
                ext = client[db_name][coll_name_list[i]].find_one({"_id": response})
                if ext:
                    print i+1
                    print ext
                    print client[db_name][coll_name_list[i]+"_matrix"].find_one({"_id": response})
                    break
                else:
                    print "not found", coll_name_list[i]
        response = raw_input().strip()
    if response == 'q':
        return False
    return True


def print_domain_line(hint):
    print "-------------------------------------------------------------------------------------"
    sdd = ["sub_l", "sub_l_e"]
    gro = ["gro_ip", "gro_p16" "gro_sub"]
    ipi = ["min_ips", "max_ips", "max_dga"]
    ttl = ["min_ttl", "max_ttl"]
    print '%5s' % "num", "|", \
        '%5s' % "p16e", "|", \
        "%8s" % "dps", "|", \
        " ".join('%8s' % tmp for tmp in sdd), "|", \
        " ".join('%8s' % tmp for tmp in gro), "|", \
        " ".join('%8s' % tmp for tmp in ipi), "|", \
        " ".join('%6s' % tmp for tmp in ttl), "|", \
        '%4s' % "life", "|", \
        "%35s" % "id", hint
    print "-------------------------------------------------------------------------------------"


def print_domain_item(item):
    print '%5d' % item['number'], "|", \
        '%5.2f' % item['idi'], "|", \
        '%8.4f' % item['dps'], "|", \
        " ".join('%8.2f' % tmp for tmp in item['sdd']), "|", \
        " ".join('%7.2f' % tmp for tmp in item['gro']), "|", \
        " ".join('%8.2f' % tmp for tmp in item['ipi']), "|", \
        " ".join('%6d' % tmp for tmp in item['ttl']), "|", \
        '%4d' % item['lifetime'], "|", \
        '%35s' % item['_id']


def print_ip_line(hint):
    print "-------------------------------------------------------------------------------------"
    dd = ["ple", "pe", "mle", "me", "sle", "se"]
    ttl = ["min_ttl", "max_ttl"]
    print '%5s' % "num", "|", \
        " ".join('%5s' % tmp for tmp in dd), "|", \
        "%4s" % "ips", "|", \
        "%4s" % "dga", "|", \
        " ".join('%6s' % tmp for tmp in ttl), "|", \
        '%4s' % "life", "|", \
        "%15s" % "id", hint
    print "-------------------------------------------------------------------------------------"


def print_ip_item(item):
    print '%5d' % item['number'], "|", \
        " ".join('%5.2f' % tmp for tmp in item['dd']), "|", \
        '%0.2f' % item['ips'], "|", \
        '%0.2f' % item['dga'], "|", \
        " ".join('%6d' % tmp for tmp in item['ttl']), "|", \
        '%4d' % item['lifetime'], "|", \
        '%15s' % item['_id']

