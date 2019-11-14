import re
import os
os.environ['OMP_NUM_THREADS'] = '1'
import subprocess as sp
import pandas as pd
from collections import defaultdict
import humanfriendly
from collections import namedtuple
from ada.users import UserTable
from ada.scontrol import ScontrolTable, ScontrolNodesTable
from ada.summary import summarize
from argparse import ArgumentParser
from datetime import timedelta
pd.set_option('expand_frame_repr', False)

time_limit = 0


def violators(stats, ut):
    cvit = stats.jobs.account('cvit')
    time_Violators_boolean = cvit.apply(lambda x: hours_used(x['RunTime'] , bool_op =1, hour_op=0), axis=1)
    time_Violators_hour = cvit.apply(lambda x: hours_used(x['RunTime'] , bool_op =0, hour_op=1), axis=1)

    
    in_long = cvit["Partition"].str.match('long')
    no_resv = cvit["Reservation"].isna() 
    encroachers = no_resv & in_long & time_Violators_boolean

    resv = cvit["Reservation"].str.match('non-deadline-queue')
    interactive = cvit["BatchFlag"].str.match('0')
    undisciplined = resv & interactive & time_Violators_boolean
    
    

    violators = cvit[undisciplined | encroachers]
    for idx, row in violators.iterrows():
        key = row['User']
        print()
        ue = ut.directory[key]
        name, email = ue.name, ue.email
        print('scancel --signal HUP {}'.format(row['JobId']), end=' ')
        print("# {} <{}> {}hrs".format(name, email,time_Violators_hour[idx]))

def info_all(stats):
    def _print(title, _object):
        print(title)
        print("-"*len(title))
        print(_object)
        print()

    _print("Summary", summarize(stats.jobs, stats.nodes))
    for account in ['cvit', 'research']:
        _print(account, stats.jobs.account(account))




def hours_used(time , bool_op , hour_op ):
    split_list = time.split(":")
    if len(split_list[0])>=4: 
        days = split_list[0].split('-')[0]
        hours = split_list[0].split('-')[1]
        minutes = split_list[1]
        seconds = split_list[2]
    else:
        days = 0
        hours = split_list[0]
        minutes = split_list[1]
        seconds = split_list[2]
    #print(int(days), int(hours),int(minutes), int(seconds))
    year = timedelta(days=int(days),hours=int(hours),minutes=int(minutes), seconds=int(seconds))
    if (bool_op and int(year.total_seconds()//3600)>time_limit):
        return 1 
    if (bool_op and int(year.total_seconds()//3600)<time_limit): 
        return 0 
    if (hour_op):
        return int(year.total_seconds()//3600) 
    
def main():
    Stats = namedtuple('Stats', 'jobs nodes')
    ut = UserTable()
    stats = Stats(jobs=ScontrolTable(), nodes=ScontrolNodesTable())
    actions = {
            "violators": lambda: violators(stats, ut),
            "cvit": lambda: print(stats.jobs.account('cvit')),
            "nlp": lambda: print(stats.jobs.account('nlp')),
            "research": lambda:  print(stats.jobs.account('research')),
            "summary": lambda: print(summarize(stats.jobs, stats.nodes)),
            "all": lambda: info_all(stats)
    }

    parser = ArgumentParser()
    ops = list(actions.keys())
    parser.add_argument('op', choices=ops, default='all', const='all', nargs='?')
    args = parser.parse_args()
    actions[args.op]()

if __name__ == '__main__':
    main()
