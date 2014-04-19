from sys import argv
from datetime import datetime, timedelta
from src.__init__ import client, coll_name_list

day_gap = 1
script, start_date_str, end_date_str = argv

this_datetime = datetime.strptime(start_date_str, "%y%m%d")
end_datetime = datetime.strptime(end_date_str, "%y%m%d")
while this_datetime < end_datetime:
    db_name = "p" + str(this_datetime.strftime("%y%m%d"))

    #client.drop_database(db_name)
    for coll_name in coll_name_list:
       client[db_name][coll_name+"_matrix"].drop()
    this_datetime = datetime.strptime(db_name[1:], "%y%m%d") + timedelta(days=day_gap)