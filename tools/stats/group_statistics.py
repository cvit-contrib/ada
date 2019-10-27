import subprocess as sp
import pandas as pd
pd.set_option('expand_frame_repr', False)
from collections import defaultdict
import humanfriendly
import re
from collections import namedtuple
from users import UserTable
from scontrol import ScontrolTable, ScontrolNodesTable
from summary import summarize
from argparse import ArgumentParser

Stats = namedtuple('Stats', 'jobs nodes')
ut = UserTable()
stats = Stats(jobs=ScontrolTable(), nodes=ScontrolNodesTable())


def violators():
    cvit = stats.jobs.account('cvit')
    in_long = cvit["Partition"].str.match('long')
    no_resv = cvit["Reservation"].isna() 
    encroachers = no_resv & in_long

    resv = cvit["Reservation"].str.match('non-deadline-queue')
    interactive = cvit["BatchFlag"].str.match('0')
    undisciplined = resv & interactive

    violators = cvit[undisciplined | encroachers]
    for idx, row in violators.iterrows():
        key = row['User']
        ue = ut.directory[key]
        name, email = ue.name, ue.email
        print('scancel --signal HUP {}'.format(row['JobId']), end=' ')
        print("# {} <{}>".format(name, email))

if __name__ == '__main__':
    actions = {
            "violators": violators,
            "cvit": lambda: print(stats.jobs.account('cvit')),
            "nlp": lambda: print(stats.jobs.account('nlp')),
            "research": lambda:  print(stats.jobs.account('research')),
            "summary": lambda: print(summarize(stats.jobs, stats.nodes))
    }

    parser = ArgumentParser()
    ops = list(actions.keys())
    parser.add_argument('op', choices=ops)
    args = parser.parse_args()
    actions[args.op]()

