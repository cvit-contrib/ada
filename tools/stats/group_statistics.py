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
    violators = cvit[cvit["Reservation"].isna()]
    for idx, row in cvit.iterrows():
        key = row['User']
        ue = ut.directory[key]
        name, email = ue.name, ue.email
        print("{} <{}>".format(name, email))

if __name__ == '__main__':
    actions = {
            "violators": violators,
            "cvit": lambda: print(stats.jobs.account('cvit')),
            "nlp": lambda: print(stats.jobs.account('nlp')),
            "research": lambda:  print(stats.jobs.account('research')),
            "summary": lambda: summarize(stats.jobs, stats.nodes)
    }
    parser = ArgumentParser()
    ops = list(actions.keys())
    parser.add_argument('op', choices=ops)
    args = parser.parse_args()
    actions[args.op]()

