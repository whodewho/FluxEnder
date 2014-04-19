from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import pylab

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


def a1():
    matrix = []
    db_name = "p140404"
    ips = client[db_name]["nor_domain"].find_one({"_id": "renren.com"})
    for ip in ips["ITEMS"]:
        for i in range(4, 8):
            t = client["p140404"][coll_name_list[i]].find_one({"_id": ip})
            if t:
                matrix.append([ip, len(t["ITEMS"]), "renren.com"])

    ips = client[db_name]["nor_domain"].find_one({"_id": "facebook.com"})
    for ip in ips["ITEMS"]:
        for i in range(4, 8):
            t = client["p140404"][coll_name_list[i]].find_one({"_id": ip})
            if t:
                matrix.append([ip, len(t["ITEMS"]), "facebook.com"])

    head = ["ip", "length", "domain"]
    df = pd.DataFrame(matrix, columns=head)
    df.boxplot('length', 'domain')
    pylab.ylabel('IP\'s Domain set Length')
    pylab.show()


def a2():
    domain_list = ["taobao.com", "qq.com", "baidu.com"]
    matrix = []
    matrixl = []
    for domain in domain_list:
        ips = client["p140404"]["nor_domain"].find_one({"_id": domain})
        for ip in ips["ITEMS"]:
            for i in range(4, 8):
                t = client["p140404"][coll_name_list[i] + "_matrix"].find_one({"_id": ip})
                if t:
                    matrix.append([t["ips"], domain])
                    matrixl.append([t["number"], domain])
                    break

    #print matrix
    df = pd.DataFrame(matrix, columns=['ips', 'domain'])
    #df = df[df['ips'] < 0.2]
    df.boxplot('ips', 'domain')
    pylab.ylabel('IP Pool Stability')
    pylab.show()

    #print matrixl
    df = pd.DataFrame(matrixl, columns=['length', 'domain'])
    #df = df[df['length'] < 100]
    df.boxplot('length', 'domain')
    pylab.ylabel('IP\'s Domain Set Size')
    pylab.show()


def a3():
    matrix = []
    db_name = "p140408"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        if domain["number"] > 60:
            continue
        matrix.append([domain["_id"], domain["number"], domain["idi"], "legit"])

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        matrix.append([domain["_id"], domain["number"], domain["idi"], "malware"])

    df = pd.DataFrame(matrix, columns=["domain", "number", "ip_diversity", "class"])
    df.boxplot('number', 'class')
    pylab.ylabel('IP Set Number')
    pylab.show()

    df.boxplot('ip_diversity', 'class')
    pylab.ylabel('IP Diversity')
    pylab.show()


def a4():
    matrix = []
    db_name = "p140408"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit", domain["number"]]
        tmp.extend(domain["gro"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware", domain["number"]]
        tmp.extend(domain["gro"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "number", "ip_growth", "prefix_growth", "subdomain_growth"])

    df.boxplot(['ip_growth', 'prefix_growth'], 'class')
    pylab.show()

    df.boxplot('subdomain_growth', 'class')
    pylab.ylabel('Subdomain Growth')
    pylab.show()

    df1 = df[df['class'] == 'legit']
    df2 = df[df['class'] == 'malware']

    plt.scatter(df1['prefix_growth'], df1['subdomain_growth'], s=120, c='#aaaaff', label='Legit', alpha=.2)
    plt.scatter(df2['prefix_growth'], df2['subdomain_growth'],s=40, c='r', label='Malware', alpha=.3)
    plt.legend()
    pylab.xlabel('IP Set Size')
    pylab.ylabel('Sub Domain Growth')
    pylab.show()


def a5():
    matrix = []
    db_name = "p140408"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        matrix.append([domain["_id"], "legit", domain["number"], domain["dps"]])

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        matrix.append([domain["_id"], "malware", domain["number"], domain["dps"]])

    df = pd.DataFrame(matrix, columns=["domain", "class", "number", "domain_stability"])

    df.boxplot('domain_stability', 'class')
    pylab.ylabel('Domain Stability')
    pylab.show()

    df1 = df[df['class'] == 'legit']
    df1 = df1[df1['number'] < 20]
    df2 = df[df['class'] == 'malware']
    df2 = df2[df2['number'] < 20]

    plt.scatter(df1['number'], df1['domain_stability'], s=120, c='#aaaaff', label='Legit', alpha=.2)
    plt.scatter(df2['number'], df2['domain_stability'], s=40, c='r', label='Malware', alpha=.3)
    plt.legend()
    pylab.xlabel('IP Set Size')
    pylab.ylabel('Sub Domain Growth')
    pylab.show()


def a6():
    matrix = []
    db_name = "p140408"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit", domain["number"]]
        tmp.extend(domain["ipi"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware", domain["number"]]
        tmp.extend(domain["ipi"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "number",
                                       "min_ip_stability", "max_ip_stability", "max_dga"])

    df.boxplot('max_ip_stability', 'class')
    pylab.ylabel('Max IP Stability')
    pylab.show()


def a7():
    matrix = []
    db_name = "p140408"
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "legit", domain["number"]]
        if domain["sdd"][0] > 60:
            continue
        tmp.extend(domain["sdd"])
        matrix.append(tmp)

    domains = client[db_name]["sus_domain_matrix"].find()
    for domain in domains:
        tmp = [domain["_id"], "malware", domain["number"]]
        if domain["sdd"][0] > 60:
            continue
        tmp.extend(domain["sdd"])
        matrix.append(tmp)

    df = pd.DataFrame(matrix, columns=["domain", "class", "number",
                                       "sdd_number", "sdd_entropy"])

    df.boxplot('sdd_number', 'class')
    pylab.ylabel('Sub Domain Number')
    pylab.show()

    df.boxplot('sdd_entropy', 'class')
    pylab.ylabel('Sub Domain Diversity')
    pylab.show()


def a8():
    matrix = []
    db_name = "p140412"
    df = pd.read_csv('../resources/top-1m.csv', names=['rank', 'domain'], header=None, encoding='utf-8')
    df = df[(df['rank'] <= 15000) & (df['rank'] >= 1)]
    rank = {}
    for index, row in df.iterrows():
        rank[row['domain']] = row['rank']
    domains = client[db_name]["nor_domain_matrix"].find()
    for domain in domains:
        if rank[domain['_id']] > 3000:
            continue
        # if domain['number'] > 100:
        #     continue
        # if domain['dps'] == 0:
        #     continue
        tmp = [domain["_id"], "legit",  rank[domain["_id"]],  domain["number"], domain["idi"], domain["dps"]]
        tmp.extend(domain["gro"])
        tmp.extend(domain["sdd"])
        tmp.extend(domain["ipi"])
        matrix.append(tmp)

    head = ["domain", "class", "rank", "number", "idi", "dps",
            "gro1", "gro2", "gro3", "sdd1", "sdd2", "ipi1", "ipi2", "ipi3"]
    df = pd.DataFrame(matrix, columns=head)
    plt.scatter(df["rank"], df["dps"], s=40, c='r', label='Legit', alpha=.3)
    plt.legend()
    pylab.xlabel('Alexa Rank')
    pylab.ylabel("Domain Pool Stability")
    pylab.show()


def init_domain_set(f, domain_set):
    for line in open(f):
        domain_set.add(line.strip())


client = MongoClient()
coll_name_list = ['domain', 'nor_domain', 'sus_domain', 'spe_domain', 'ip', 'nor_ip', 'sus_ip', 'spe_ip']
a7()