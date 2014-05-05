from pymongo import MongoClient
import pandas as pd
import numpy as np
import pylab
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from log_to_mongo_lib import init_from_phishtank

def init_from_csv(begin=1, end=10000):
    df = pd.read_csv('top-1m.csv', names=['rank', 'domain'], header=None, encoding='utf-8')
    return set(list(df[(df['rank'] <= end) & (df['rank'] >= begin)]['domain']))


def init_domain_set(f, domain_set):
    for line in open(f):
        domain_set.add(line.strip('\n'))


def get_list(_row, _class):
    tmp = [_row["_id"], _row["number"], _row["idi"], _row["dps"]]
    tmp.extend(_row["sdd"])
    tmp.extend(_row["gro"])
    tmp.extend(_row["ipi"])
    tmp.extend(_row["ttl"])
    tmp.append(_class)
    return tmp


def show_cm(_cm, _labels):
    percent = (_cm*100.0)/np.array(np.matrix(_cm.sum(axis=1)).T)  # Derp, I'm sure there's a better way
    print 'Confusion Matrix Stats'
    for i, label_i in enumerate(_labels):
        for j, label_j in enumerate(_labels):
            print "%s/%s: %.2f%% (%d/%d)" % (label_i, label_j, (percent[i][j]), _cm[i][j], _cm[i].sum())


def init_bot_domain():
    sus_domain_set = set([])
    init_domain_set("../resources/hosts_badzeus.txt", sus_domain_set)
    init_domain_set("../resources/hosts_spyeye.txt", sus_domain_set)
    init_domain_set("../resources/hosts_palevo.txt", sus_domain_set)
    init_domain_set("../resources/hosts_feodo.txt", sus_domain_set)
    init_domain_set("../resources/hosts_cybercrime.txt", sus_domain_set)
    return sus_domain_set


def a1(alpha1=9, alpha2=0.2, alpha3=12000, db_name="p140414"):
    matrix = []
    m_num, l_num = 0, 0
    bot_domain_set = init_bot_domain()

    cursor = client[db_name]["nor_domain_matrix"].find()
    for row in cursor:
        l_num += 1
        matrix.append(get_list(row, "legit"))

    cursor = client[db_name]["sus_domain_matrix"].find()
    for row in cursor:
        if row['_id'] not in bot_domain_set and row['number'] < alpha1 or row['idi'] < alpha2 or row['ttl'][0] > alpha3:
            continue
        m_num += 1
        matrix.append(get_list(row, "malware"))

    if m_num < 200 or m_num/float(l_num) < 0.1:
        return [[0, 0], [0, 0]]

    df = pd.DataFrame(matrix, columns=head)
    df = df.reindex(np.random.permutation(df.index))
    X = df.as_matrix(head[:14])
    y = np.array(df["class"].tolist())

    # import sklearn.ensemble
    # clf = sklearn.ensemble.RandomForestClassifier(n_estimators=20)
    # scores = sklearn.cross_validation.cross_val_score(clf, X, y, cv=10, n_jobs=-1)
    # print scores
    # rdf = pd.DataFrame(scores, columns=["Precision"])
    # rdf.plot()

    # importances = zip(head[1:14], clf.feature_importances_)
    # print importances

    import sklearn.ensemble
    clf = sklearn.ensemble.RandomForestClassifier(n_estimators=20)
    from sklearn.cross_validation import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    NX_train = [t[1: 14] for t in X_train]
    NX_test = [t[1: 14] for t in X_test]

    clf.fit(NX_train, y_train)
    y_pred = clf.predict(NX_test)

    # for k in range(len(y_pred)):
    #     if y_pred[k] != y_test[k]:
    #         tmp = NX_test[k].tolist()
    #         d = X_test[k][0]
    #         if y_pred[k] == 'malware' and y_test[k] == 'legit':
    #             if d not in fp:
    #                 fp[d] = 1
    #             else:
    #                 fp[d] += 1
    #             tmp.append('flase_positive')
    #         elif y_pred[k] == 'legit' and y_test[k] == 'malware':
    #             if d not in fn:
    #                 fn[d] = 1
    #             else:
    #                 fn[d] += 1
    #             tmp.append('false_negative')
    #         f_matrix.append(tmp)

    from sklearn.metrics import confusion_matrix
    labels = ['legit', 'malware']
    cm = confusion_matrix(y_test, y_pred, labels)
    percent = (cm*100.0)/np.array(np.matrix(cm.sum(axis=1)).T)
    # t_matrix.append([percent[0][0], percent[1][1], db_name[1:]])

    # if percent[0][0] > 90 and percent[1][1] > 80:
    #     print m_num, l_num
    #     print 'Confusion Matrix Stats'
    #     for i, label_i in enumerate(labels):
    #         for j, label_j in enumerate(labels):
    #             print "%s/%s: %.2f%% (%d/%d)" % (label_i, label_j, (percent[i][j]), cm[i][j], cm[i].sum())
    # fig = plt.figure()
    # ax = fig.add_subplot(111)
    # ax.grid(b=False)
    # cax = ax.matshow(percent, cmap='coolwarm')
    # pylab.title('Confusion matrix of the classifier')
    # fig.colorbar(cax)
    # ax.set_xticklabels([''] + labels)
    # ax.set_yticklabels([''] + labels)
    # pylab.xlabel('Predicted')
    # pylab.ylabel('True')
    # pylab.show()

    return percent


def a2(db_name):
    max_tp = 0
    max_tn = 0
    alpha = []
    for alpha1 in range(2, 20, 1):
        for alpha2 in pylab.frange(0.1, 0.5, 0.1):
            for alpha3 in range(1000, 30000, 1000):
                percent = a1(alpha1, alpha2, alpha3, db_name)
                if percent[0][0] > max_tp and percent[1][1] > max_tn:
                    max_tp = percent[0][0]
                    max_tn = percent[1][1]
                    alpha = [alpha1, alpha2, alpha3]
    t_matrix.append([max_tp, max_tn, db_name[1:]])
    print alpha, db_name, max_tp, max_tn


def a3():
    this_datetime = datetime(2014, 4, 2)
    end_datetime = datetime(2014, 4, 26)
    while this_datetime < end_datetime:
        db_name = "p" + str(this_datetime.strftime("%y%m%d"))
        print db_name
        a2(db_name)
        this_datetime = datetime.strptime(db_name[1:], "%y%m%d") + timedelta(days=1)

    df = pd.DataFrame(t_matrix, columns=["true_positive", "true_negative", "date"])
    df.plot(x='date')
    pylab.show()


client = MongoClient()
matrix_list = ['domain', 'nor_domain', 'sus_domain']
head = ["domain", "number", "ip_diversity", "domain_pool_stability",
        "subdomain_number", "subdomain_diversity",
        "ip_growth", "p16_growth", "subdomain_growth",
        "min_ip_pool_stability", "max_ip_pool_stability", "max_dga",
        "min_ttl", "max_ttl",
        "class"]

fp = {}
fn = {}

f_matrix = []
t_matrix = []

# for m in range(50):
#     print m
#     a1(db_name="p140414")

a3()


def plot_false():
    with open("../output/fp.txt", "w") as writer:
        for d in fp:
            if fp[d] > 20:
                print d, fp[d]
                writer.write(d + "\n")
    print "---------------------------"
    with open("../output/fn.txt", "w") as writer:
        for d in fn:
            if fn[d] > 15:
                print d, fn[d]
                writer.write(d + "\n")


    df = pd.DataFrame(f_matrix, columns=head[1:15])
    df = df[df['number'] < 100]
    df = df[df['subdomain_number'] < 100]
    df.boxplot("number", by='class')
    pylab.show()

    df.boxplot(["ip_diversity", "p16_growth", "subdomain_growth", "max_ip_pool_stability"], by='class')
    pylab.show()

    df = df[df['min_ttl'] < 2000]
    df = df[df['max_ttl'] < 2000]
    df.boxplot(head[12: 14], by='class')
    pylab.show()
