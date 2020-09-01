import re
import os
os.environ['OMP_NUM_THREADS'] = '1'
import subprocess as sp
import pandas as pd
from collections import defaultdict
import humanfriendly
from collections import namedtuple
from .users import UserTable
from .scontrol import ScontrolTable, ScontrolNodesTable
from .summary import summarize
from argparse import ArgumentParser
from datetime import timedelta
import math
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', None)


time_limit = 0
cpu_limit_wo_gpu = 5
cpu_gpu_ratio_limit = 10

def violators(stats, ut):
    cvit = stats.jobs.account('cvit')
    
    time_Violators_boolean = cvit.apply(lambda x: hours_used(x['RunTime'] , bool_op =1, hour_op=0), axis=1)
    time_Violators_hour = cvit.apply(lambda x: hours_used(x['RunTime'] , bool_op =0, hour_op=1), axis=1)

    resource_violators_boolean =  cvit.apply(lambda x: resource_used(x['TRES'],bool_op =1, resource_op=0), axis=1)
    resource_violators =  cvit.apply(lambda x: resource_used(x['TRES'],bool_op =0, resource_op=1), axis=1)
    

    in_long = cvit["Partition"].str.match('long')
    #no_resv = cvit["Reservation"].isna() 
    #encroachers = no_resv & in_long & time_Violators_boolean

    #resv = cvit["Reservation"].str.match('non-deadline-queue')
    interactive = cvit["BatchFlag"].str.match('0')
    undisciplined_bash = in_long & interactive & time_Violators_boolean
    undisciplined_resource = in_long & resource_violators_boolean
    
    

    violators_bash = cvit[undisciplined_bash]
    for idx, row in violators_bash.iterrows():
        key = row['User']
        ue = ut.directory[key]
        name, email = ue.name, ue.email
        print('Bash Violators scancel --signal HUP {}'.format(row['JobId']), end=' ')
        print("# {} <{}> {}hrs ,{}".format(name, email,time_Violators_hour[idx],resource_violators[idx]))

    violators_resources = cvit[undisciplined_resource]
    print('\n')
    for idx, row in violators_resources.iterrows():
        key = row['User']
        ue = ut.directory[key]
        name, email = ue.name, ue.email
        print('Resource Violators scancel --signal HUP {}'.format(row['JobId']), end=' ')
        print("# {} <{}> {}".format(name, email,resource_violators[idx]))

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
    if (bool_op and int(year.total_seconds()//3600)>=time_limit):
        return 1 
    if (bool_op and int(year.total_seconds()//3600)<time_limit): 
        return 0 
    if (hour_op):
        return int(year.total_seconds()//3600) 

def resource_used(TRES , bool_op , resource_op):
    split_list = TRES.split(",")
    cpu_used = int(split_list[0].split("=")[-1])
    
    if (bool_op and 'gpu' in TRES):
        gpu_used = int(split_list[-1].split("=")[-1])
        if (math.ceil(cpu_used/gpu_used))>cpu_gpu_ratio_limit:
            return 1
        else:
            return 0
    
    if (bool_op and 'gpu' not in TRES):
        if(cpu_used>cpu_limit_wo_gpu): 
            return 1
        else:
            return 0
    
    if (resource_op and 'gpu' in TRES):
        gpu_used = int(split_list[-1].split("=")[-1])
        str_out = '(c'+ ' ' + str(cpu_used)+ '),' + '(g' + ' ' + str(gpu_used) + ')'
        return str_out
    if (resource_op and 'gpu' not in TRES):
        str_out = '(c'+ ' ' + str(cpu_used)+ '),' + '(g' + ' ' + str(0) + ')'
        return str_out
        
    

    
    
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
