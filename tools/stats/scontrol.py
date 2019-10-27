import subprocess as sp
import pandas as pd
from collections import defaultdict
import humanfriendly
import re
from collections import namedtuple
from users import UserTable

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
            'BatchFlag',
            'Reservation'
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
        # self.df = self.df.dropna()
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

    def remove(self, df):
        keys = ['GroupId', 'UserId', "gres/gpu", "mem", "cpu", "billing", 'JobState', 'node', 'Account']
        active_keys = list(df.columns)
        intersection = list(set(keys).intersection(set(active_keys)))
        return df.drop(intersection, axis=1)

    
    def cvit(self):
        idxau = self.df["Account"] == self.df["User"]
        idxcvit = 'cvit' == self.df["Group"] 
        idx = idxau & idxcvit
        return self.df[idx]
    
    def account(self, account, remove=True):
        if account == 'cvit':
            x = self.cvit()
        
        else:
            idx = self.df["Account"] == account
            x = self.df[idx]

        return x if not remove else self.remove(x)

    
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
