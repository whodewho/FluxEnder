# -*- coding: UTF-8 -*-
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import pylab
from datetime import datetime, timedelta


print 'Pandas version: %s' % pd.__version__

def a0():
    db_name = "p140404"
    head = ["ip", "number", "dd_p_l", "dd_p", "dd_m_l", "dd_m", "dd_s_l", "dd_s"]
    matrix1 = []
    cursor = client[db_name]["nor_ip_matrix"].find()
    for row in cursor:
        tmp = [row["_id"], row["number"]]
        tmp.extend(row["dd"])
        matrix1.append(tmp)
    df = pd.DataFrame(matrix1, columns=head)
    df = df[df['number'] < 1000]
    plt.scatter(df['number'], df['dd_m_l'], s=140, c='#aaaaff', label="Legit", alpha=.2)

    matrix2 = []
    cursor = client[db_name]["spe_ip_matrix"].find()
    for row in cursor:
        tmp = [row["_id"], row["number"]]
        tmp.extend(row["dd"])
        matrix2.append(tmp)
    df = pd.DataFrame(matrix2, columns=head)
    df = df[df['number'] < 1000]
    plt.scatter(df['number'], df['dd_m_l'], s=40, c='r', label='Sell', alpha=.3)

    plt.legend()
    pylab.xlabel("domain set size")
    pylab.ylabel("domain diversity")
    pylab.show()
#
# def a1():
#     matrix = []
#     db_name = "p140404"
#     ips = client[db_name]["nor_domain"].find_one({"_id": "renren.com"})
#     for ip in ips["ITEMS"]:
#         for i in range(4, 8):
#             t = client["p140404"][coll_name_list[i]].find_one({"_id": ip})
#             if t:
#                 matrix.append([ip, len(t["ITEMS"]), "renren.com"])
#
#     ips = client[db_name]["nor_domain"].find_one({"_id": "facebook.com"})
#     for ip in ips["ITEMS"]:
#         for i in range(4, 8):
#             t = client["p140404"][coll_name_list[i]].find_one({"_id": ip})
#             if t:
#                 matrix.append([ip, len(t["ITEMS"]), "facebook.com"])
#
#     head = ["ip", "length", "domain"]
#     df = pd.DataFrame(matrix, columns=head)
#     df.boxplot('length', 'domain')
#     pylab.ylabel('IP\'s Domain set Length')
#     pylab.show()
#
#
# def a2():
#     domain_list = ["taobao.com", "qq.com", "baidu.com"]
#     matrix = []
#     matrixl = []
#     for domain in domain_list:
#         ips = client["p140404"]["nor_domain"].find_one({"_id": domain})
#         for ip in ips["ITEMS"]:
#             for i in range(4, 8):
#                 t = client["p140404"][coll_name_list[i] + "_matrix"].find_one({"_id": ip})
#                 if t:
#                     matrix.append([t["ips"], domain])
#                     matrixl.append([t["number"], domain])
#                     break
#
#     #print matrix
#     df = pd.DataFrame(matrix, columns=['ips', 'domain'])
#     #df = df[df['ips'] < 0.2]
#     df.boxplot('ips', 'domain')
#     pylab.ylabel('IP Pool Stability')
#     pylab.show()
#
#     #print matrixl
#     df = pd.DataFrame(matrixl, columns=['length', 'domain'])
#     #df = df[df['length'] < 100]
#     df.boxplot('length', 'domain')
#     pylab.ylabel('IP\'s Domain Set Size')
#     pylab.show()


def a1():
    matrix = []
    db_name = "p140428"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        matrix.append([domain["_id"], domain["ip_count"], domain["p16_entropy"], "legit"])

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        matrix.append([domain["_id"], domain["ip_count"], domain["p16_entropy"], "malware"])

    df = pd.DataFrame(matrix, columns=["domain", "ip_count", "p16_entropy", "class"])
    df = df[df["ip_count"] < 50]
    df.boxplot('ip_count', 'class')
    pylab.ylabel('IP Count')
    pylab.show()

    df.boxplot('p16_entropy', 'class')
    pylab.ylabel("IP Diversity")
    pylab.show()


def a2():
    matrix = []
    db_name = "p140428"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit"]
        tmp.extend(domain["subdomain"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware"]
        tmp.extend(domain["subdomain"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "subd_count", "subd_length_entropy"])
    df = df[df["subd_count"] < 50]

    df.boxplot('subd_count', 'class')
    pylab.ylabel('Subdomain Number')
    pylab.show()

    df.boxplot('subd_length_entropy', 'class')
    pylab.ylabel('Subdomain Diversity')
    pylab.show()


def a3():
    matrix = []
    db_name = "p140428"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit"]
        tmp.extend(domain["ttl"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware"]
        tmp.extend(domain["ttl"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "min_ttl", "max_ttl", "diff_ttl"])

    df = df[df["min_ttl"] < 10000]
    df.boxplot('min_ttl', 'class')
    pylab.ylabel('Min TTL')
    pylab.show()

    df = df[df["max_ttl"] < 40000]
    df.boxplot('max_ttl', 'class')
    pylab.ylabel('Max TTL')
    pylab.show()

    df.boxplot('diff_ttl', 'class')
    pylab.ylabel('Diff TTL')
    pylab.show()

#
# def a4():
#     matrix = []
#     db_name = "p140408"
#     domains = client[db_name]["nor_domain_matrix"].find()
#     for domain in domains:
#         tmp = [domain["_id"], "legit", domain["number"]]
#         tmp.extend(domain["gro"])
#         matrix.append(tmp)
#
#     domains = client[db_name]["sus_domain_matrix"].find()
#     for domain in domains:
#         tmp = [domain["_id"], "malware", domain["number"]]
#         tmp.extend(domain["gro"])
#         matrix.append(tmp)
#
#     df = pd.DataFrame(matrix, columns=["domain", "class", "number", "ip_growth", "prefix_growth", "subdomain_growth"])
#     df.boxplot(['ip_growth', 'prefix_growth', 'subdomain_growth'], 'class')
#     pylab.show()
#
#     # df.boxplot('subdomain_growth', 'class')
#     # pylab.ylabel('Subdomain Growth')
#     # pylab.show()
#     #
#     # df1 = df[df['class'] == 'legit']
#     # df2 = df[df['class'] == 'malware']
#     #
#     # plt.scatter(df1['prefix_growth'], df1['subdomain_growth'], s=120, c='#aaaaff', label='Legit', alpha=.2)
#     # plt.scatter(df2['prefix_growth'], df2['subdomain_growth'],s=40, c='r', label='Malware', alpha=.3)
#     # plt.legend()
#     # pylab.xlabel('IP Set Size')
#     # pylab.ylabel('Sub Domain Growth')
#     # pylab.show()


def a4():
    matrix = []
    db_name = "p140430"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit", domain["ip_count"]]
        tmp.extend(domain["growth"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware", domain["ip_count"]]
        tmp.extend(domain["growth"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "number", "p16_growth_1", "p16_growth_4", "p16_growth_8",
                                       "subd_growth_1", "subd_growth_4", "subd_growth_8"])
    df.boxplot(['p16_growth_1', 'p16_growth_4', 'p16_growth_8', "subd_growth_1", "subd_growth_4", "subd_growth_8"], 'class')
    pylab.show()


def a5():
    matrix = []
    db_name = "p140427"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit", domain["ip_count"]]
        tmp.extend(domain["relative"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware", domain["ip_count"]]
        tmp.extend(domain["relative"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "ip_count", "relative_domain_count", "unique_2ld_ratio",
                                       "unique_ip_ratio"])

    df.boxplot('relative_domain_count', 'class')
    pylab.ylabel('Relative Domain Count')
    pylab.show()

    df.boxplot(['unique_2ld_ratio', 'unique_ip_ratio'], 'class')
    pylab.show()

    #
    # df1 = df[df['class'] == 'legit']
    # df1 = df1[df1['number'] < 20]
    # df2 = df[df['class'] == 'malware']
    # df2 = df2[df2['number'] < 20]
    #
    # plt.scatter(df1['relative_domain_count'], df1['unique_2ld_ratio'], s=120, c='#aaaaff', label='Legit', alpha=.2)
    # plt.scatter(df2['relative_domain_count'], df2['unique_2ld_ratio'], s=40, c='r', label='Malware', alpha=.3)
    # plt.legend()
    # pylab.xlabel('IP Set Size')
    # pylab.ylabel('Sub Domain Growth')
    # pylab.show()


# def a6():
#     matrix = []
#     db_name = "p140408"
#     domains = client[db_name]["nor_domain_matrix"].find()
#     for domain in domains:
#         tmp = [domain["_id"], "legit", domain["number"]]
#         tmp.extend(domain["ipi"])
#         matrix.append(tmp)
#
#     domains = client[db_name]["sus_domain_matrix"].find()
#     for domain in domains:
#         tmp = [domain["_id"], "malware", domain["number"]]
#         tmp.extend(domain["ipi"])
#         matrix.append(tmp)
#
#     df = pd.DataFrame(matrix, columns=["domain", "class", "number",
#                                        "min_ip_stability", "max_ip_stability", "max_dga"])
#
#     df.boxplot('max_ip_stability', 'class')
#     pylab.ylabel('Max IP Stability')
#     pylab.show()
def a6():
    matrix = []
    db_name = "p140428"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit", domain["ip_count"]]
        tmp.extend(domain["ipinfo"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware", domain["ip_count"]]
        tmp.extend(domain["ipinfo"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "number",
                                       "min_ip_stability", "max_ip_stability", "max_dga_ratio"])

    df.boxplot('max_dga_ratio', 'class')
    pylab.ylabel('Max DGA ratio')
    pylab.show()

#
# def a8():
#     matrix = []
#     db_name = "p140412"
#     df = pd.read_csv('../resources/top-1m.csv', names=['rank', 'domain'], header=None, encoding='utf-8')
#     df = df[(df['rank'] <= 15000) & (df['rank'] >= 1)]
#     rank = {}
#     for index, row in df.iterrows():
#         rank[row['domain']] = row['rank']
#     domains = client[db_name]["nor_domain_matrix"].find()
#     for domain in domains:
#         if rank[domain['_id']] > 3000:
#             continue
#         # if domain['number'] > 100:
#         #     continue
#         # if domain['dps'] == 0:
#         #     continue
#         tmp = [domain["_id"], "legit",  rank[domain["_id"]],  domain["number"], domain["idi"], domain["dps"]]
#         tmp.extend(domain["gro"])
#         tmp.extend(domain["sdd"])
#         tmp.extend(domain["ipi"])
#         matrix.append(tmp)
#
#     head = ["domain", "class", "rank", "number", "idi", "dps",
#             "gro1", "gro2", "gro3", "sdd1", "sdd2", "ipi1", "ipi2", "ipi3"]
#     df = pd.DataFrame(matrix, columns=head)
#     plt.scatter(df["rank"], df["dps"], s=40, c='r', label='Legit', alpha=.3)
#     plt.legend()
#     pylab.xlabel('Alexa Rank')
#     pylab.ylabel("Domain Pool Stability")
#     pylab.show()


def init_domain_set(f, domain_set):
    for line in open(f):
        domain_set.add(line.strip())


def b1():
    this_datetime = datetime(2014, 3, 1)
    end_datetime = datetime(2014, 3, 19)
    matrix = []
    feature = "ip_count"
    while this_datetime < end_datetime:
        db_name = "p" + str(this_datetime.strftime("%y%m%d"))
        print db_name
        domains = client[db_name]["nor_domain_matrix"].find()
        for domain in domains:
            matrix.append([domain["_id"], domain[feature], "legit", db_name[1:]])

        domains = client[db_name]["sus_domain_matrix"].find()
        for domain in domains:
            matrix.append([domain["_id"], domain[feature], "malware", db_name[1:]])

        this_datetime = datetime.strptime(db_name[1:], "%y%m%d") + timedelta(days=1)

    df = pd.DataFrame(matrix, columns=["domain", feature, "class", "date"])
    df = df[df['class'] == 'malware']
    df.boxplot(feature, 'date')
    pylab.show()


def b2():
    matrix = []
    db_name = "p140228"
    df = pd.read_csv('../resources/top-1m.csv', names=['rank', 'domain'], header=None, encoding='utf-8')
    df = df[(df['rank'] <= 10000) & (df['rank'] >= 1)]
    rank = {}
    for index, row in df.iterrows():
        rank[row['domain']] = row['rank']
    domains = client[db_name]["nor_domain_matrix"].find()
    feature = 'ip_count'
    for domain in domains:
        matrix.append([domain["_id"], domain[feature], rank[domain["_id"]]])

    df = pd.DataFrame(matrix, columns=["domain", feature, 'rank'])
    # df = df[df[feature] < 8000]
    plt.scatter(df["rank"], df[feature], s=20, c='r', label='Alexa Top 10000', alpha=.3)
    plt.legend()
    pylab.xlabel('Alexa Rank')
    pylab.ylabel(feature)
    pylab.show()


def get_list(_row, _class):
    tmp = [_row["_id"], _row["ip_count"], _row["p16_entropy"]]
    tmp.extend(_row["relative"])
    tmp.extend(_row["subdomain"])
    tmp.extend(_row["growth"])
    tmp.extend(_row["ipinfo"])
    tmp.extend(_row["ttl"])
    tmp.append(_class)
    return tmp


def b3():
    this_datetime = datetime(2014, 3, 1)
    end_datetime = datetime(2014, 3, 18)
    matrix = []
    while this_datetime < end_datetime:
        db_name = "p" + str(this_datetime.strftime("%y%m%d"))
        print db_name

        domains = client[db_name]["nor_domain_matrix"].find()
        for domain in domains:
            if domain['ip_count'] < 2 or domain['ttl'][0] > 20000 or domain['p16_entropy'] < 0.08:
                continue
            li = get_list(domain, "legit")
            li.append(db_name[5:])
            matrix.append(li)

        domains = client[db_name]["sus_domain_matrix"].find()
        for domain in domains:
            if domain['ip_count'] < 2 or domain['ttl'][0] > 20000 or domain['p16_entropy'] < 0.08:
                continue
            li = get_list(domain, "malware")
            li.append(db_name[5:])
            matrix.append(li)

        this_datetime = datetime.strptime(db_name[1:], "%y%m%d") + timedelta(days=1)

    df = pd.DataFrame(matrix, columns=head)

    # fig, axes = plt.subplot(nrows=1, ncols=2)
    # df[df['class'] == 'malware'].boxplot('ip_count', 'date', ax=axes[0, 0])
    # df[df['class'] == 'malware'].boxplot('p16_entropy', 'date', ax=axes[0, 1])
    # pylab.show()

    df[df['class'] == 'legit'].boxplot('subd_growth_8', 'date')
    df[df['class'] == 'malware'].boxplot('subd_growth_8', 'date')
    pylab.show()


def b4():
    this_datetime = datetime(2014, 3, 1)
    end_datetime = datetime(2014, 3, 18)
    matrix = []
    i = 1
    while this_datetime < end_datetime:
        db_name = "q" + str(this_datetime.strftime("%y%m%d"))
        print db_name
        matrix.append([client[db_name]["nor_domain"].count(),
                       client[db_name]["sus_domain"].count(),
                       client["p" + str(this_datetime.strftime("%y%m%d"))]["sus_domain"].count(), i])
        i += 1
        this_datetime = datetime.strptime(db_name[1:], "%y%m%d") + timedelta(days=1)

    df = pd.DataFrame(matrix, columns=["legit domain count", "botnet domain count", "malware domain count", "day"])
    # plt.plot(df['day'], df['legit domain count'], 'r--',
    #          df['day'], df['botnet domain count'], 'bs',
    #          df['day'], df['malware domain count'], 'g^')
    df.plot(x='day', style=['k', 'r--', 'g^'])
    pylab.show()


def b5():
    matrix = []
    for i in range(2, 26):
        matrix.append([0])
    matrix[2] = [1]
    matrix[3] = [2]
    matrix[6] = [1]
    matrix[15] = [1]
    matrix[16] = [1]
    for i in range(2, 26):
        matrix[i-2].append(i)
    df = pd.DataFrame(matrix, columns=["malware count", "day"]);
    df.plot(x='day')
    pylab.show()


def b6():
    db_name = 'p140426'
    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        if domain['relative'][2] > 0.2:
            print domain



head = ["domain", "ip_count", "p16_entropy",
        "relative_domain_count", "unique_2ld_ratio", "unique_ip_ratio",
        "subd_count", "subd_length_entropy",
        "p16_growth_1", "p16_growth_4", "p16_growth_8",
        "subd_growth_1", "subd_growth_4", "subd_growth_8",
        "min_unique_domain_ratio", "max_unique_domain_ratio", "max_dga_ratio",
        "min_ttl", "max_ttl", "diff_ttl",
        "class", "date"]

client = MongoClient()
coll_name_list = ['domain', 'nor_domain', 'sus_domain', 'spe_domain', 'ip', 'nor_ip', 'sus_ip', 'spe_ip']
b3()


