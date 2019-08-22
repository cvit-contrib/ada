import subprocess as sp
import pandas as pd
from collections import defaultdict
import humanfriendly
import re
from collections import namedtuple


class ScontrolEntry:
    def __init__(self, _string):
        d = self.parse(_string)
        self.df = pd.DataFrame.from_dict(d)
        pattern = re.compile('(.*)\([0-9]*\)')
        f = lambda x: pattern.match(x).group(1)
        
        self.df["User"] = self.df["UserId"].apply(f)
        self.df["Group"] = self.df["GroupId"].apply(f)
        self.df = self.df.fillna(0)
        
    def parse(self, line):
        required = [
            'TRES', 'JobId', 
            # 'SubmitTime', 
            'RunTime',
            'UserId',
            'Account', 
            'GroupId', 'JobState', 
            'BatchFlag'
        ]
        entries = line.split()
        _dict = {}
        for entry in entries:
            key, *values = entry.split("=")
            if key in required:
                _dict[key] = '='.join(values)
        for key in _dict:
            _dict[key] = [_dict[key]]
        return _dict
    
    def __repr__(self):
        return self.df.__repr__()
    
class ScontrolTable:
    def __init__(self):
        output = sp.check_output("scontrol show jobs -o", shell=True).decode("utf-8")
        ses = [ScontrolEntry(line).df for line in output.splitlines()]
        self.df = pd.concat(ses, axis=0)
        self.df = self.df.dropna()
        tres = ScontrolTable.computeTRES(self.df)
        self.df = pd.merge(tres, self.df, on='JobId')
        self.df = self.df[self.df["JobState"] == "RUNNING"]
        self._approx()

    def _approx(self):
        M = humanfriendly.parse_size('126G')
        ndf = pd.DataFrame()
        ndf["cpu"] = self.df["cpu"]/40
        ndf["mem"] = self.df["mem"]/M
        ndf["gres/gpu"] = self.df["gres/gpu"]/4
        end = len(self.df.columns)
        self.df.insert(end, 'node-equivalent', value=ndf.max(axis=1))
        # self.df.assign(node_equivalent=ndf.values)
        # self.df["node-equivalent"] = ndf.max(axis=1)

    
    def cvit(self):
        idxau = self.df["Account"] == self.df["User"]
        idxcvit = 'cvit' == self.df["Group"] 
        idx = idxau & idxcvit
        return self.df[idx]
    
    def account(self, account):
        if account == 'cvit':
            return self.cvit()
        
        idx = self.df["Account"] == account
        return self.df[idx]

    
    @staticmethod
    def computeTRES(df):
        def parse(text):
            keyvals = text.split(',')
            _d = defaultdict(lambda: None)
            for keyval in keyvals:
                key, value = keyval.split("=")
                _d[key] = value
                if key == 'mem':
                    if value.isnumeric():
                        value = '{}M'.format(value)
                    _d[key] = humanfriendly.parse_size(value)
                _d[key] = [int(_d[key])]
            return pd.DataFrame.from_dict(_d)
        
        tdf = df["TRES"].apply(parse)
        ndf = pd.concat(tdf.tolist(), axis=0)
        ndf["JobId"] = df["JobId"]
        return ndf
    




class ScontrolNodesTable:
    def __init__(self):
        output = sp.check_output("scontrol show nodes -o", shell=True).decode("utf-8")
        dfs = [ScontrolNodesTable.parse(line) for line in output.splitlines()]
        self.df = pd.concat(dfs, axis=0)
        for tres in ['cpu', 'mem', 'gres/gpu']:
            g = lambda x: '{}-{}'.format(x, tres)
            self.df[g("free")] = self.df[g("CfgTRES")] - self.df[g("AllocTRES")]

    def _acc_idxs(self, df):
        idxs = None
        for tres in ['cpu', 'mem', 'gres/gpu']:
            idx = self.df["free-" + tres] != 0
            if idxs is None:
                idxs = idx
            else:
                idxs = idxs & idx
        return idxs
    
    def accessible(self):
        idxs = self._acc_idxs(self.df)
        return self.df[idxs]

    def ghost(self):
        idxs =  self._acc_idxs(self.df)
        return self.df[~idxs]

            
    @staticmethod
    def parseTRES(TRES):
        state = defaultdict(lambda : '0')
        for keyval in TRES.split(','):
            if keyval:
                key, val = keyval.split('=')
                state[key] = val

        # print(state)
        state["cpu"] = int(state["cpu"])
        state["mem"] = humanfriendly.parse_size(state["mem"])
        state["gres/gpu"] = int(state["gres/gpu"])
        return state
    
    @staticmethod
    def parse(line):
        required = [
            'NodeName', 'CfgTRES', 'AllocTRES'
        ]
        entries = line.split()
        _dict = {}
        for entry in entries:
            key, *values = entry.split("=")
            value = '='.join(values)
            if key in required:
                if 'TRES' in key:
                    state = ScontrolNodesTable.parseTRES(value)
                    for treskey in state:
                        _dict[key + '-' +treskey] = state[treskey]
                else:
                    _dict[key] = value
        
        for key in _dict:
            _dict[key] = [_dict[key]]
        return pd.DataFrame.from_dict(_dict)



Stats = namedtuple('Stats', 'jobs nodes')
stats = Stats(jobs=ScontrolTable(), nodes=ScontrolNodesTable())

def summarize(jobs_table, nodes_table):
    table = jobs_table
    nt = nodes_table
    accessible = nt.accessible()
    ghost = nt.ghost()
    d = defaultdict(list)
    cumulative = defaultdict(float)
    ratios = {"cpu": 40, "mem": humanfriendly.parse_size('126G'), "gres/gpu": 4}
    for account in ['cvit', 'nlp', 'ccnsb', 'research', 'sub', 'mll']:
        df = table.account(account)
        d['Account'].append(account)
        for key in ['cpu', 'mem', 'gres/gpu']:
            value = df[key].sum()/ratios[key]
            cumulative[key] += value
            value = round(value, 2)
            d[key].append(value)

        d['upper-bound'].append(df["node-equivalent"].sum())

    d['Account'].append('total-usage')
    for key in ['cpu', 'mem', 'gres/gpu']:
        value = round(nt.df['AllocTRES-'+ key].sum()/ratios[key], 2)
        d[key].append(value)


    d['Account'].append('total-alloc')
    for key in ['cpu', 'mem', 'gres/gpu']:
        value = round(cumulative[key], 2)
        d[key].append(value)

    d['Account'].append('accessible')
    for key in ['cpu', 'mem', 'gres/gpu']:
        value = round(accessible['free-'+ key].sum()/ratios[key], 2)
        d[key].append(value)

    d['Account'].append('ghost')
    for key in ['cpu', 'mem', 'gres/gpu']:
        value = round(ghost['free-'+ key].sum()/ratios[key], 2)
        d[key].append(value)

    while(len(d['upper-bound']) < len(d['Account'])):
        d['upper-bound'].append('-')


    summary = pd.DataFrame.from_dict(d)
    return summary

if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)
    print(stats.jobs.cvit())
    print()
    print(stats.jobs.account('nlp'))
    print()
    print(stats.jobs.account('ccnsb'))
    print()
    print(stats.jobs.account('research'))
    print()
    summary = summarize(stats.jobs, stats.nodes)
    print(summary)

    # print(stats.nodes.accessible())

