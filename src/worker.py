import os
from sys import argv
from datetime import datetime, timedelta
from __init__ import day_gap

script, start_date_str, end_date_str = argv

this_datetime = datetime.strptime(start_date_str, "%y%m%d")
end_datetime = datetime.strptime(end_date_str, "%y%m%d")


while this_datetime < end_datetime:
    print ("python log_to_mongo.py " + str(this_datetime.strftime("%y%m%d")) + " " + str(day_gap))
    this_datetime = this_datetime + timedelta(days=day_gap)


this_datetime = datetime.strptime(start_date_str, "%y%m%d")
while this_datetime < end_datetime:
    db_name = "p" + str(this_datetime.strftime("%y%m%d"))
    print ("python extract_feature.py " + db_name)
    this_datetime = datetime.strptime(db_name[1:], "%y%m%d") + timedelta(days=day_gap)