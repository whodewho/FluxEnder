from src.extract_feature_lib import handle_exception, client, domain_p, ip_p, \
    print_domain_item, print_domain_line, print_ip_item, print_ip_line, coll_name_list
import time
from datetime import datetime

domain_coll = ["domain", "sus_domain", "nor_domain"]
ip_coll = ["ip", "sus_ip", "nor_ip"]


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
                for coll_name in domain_coll:
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
                for coll_name in ip_coll:
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