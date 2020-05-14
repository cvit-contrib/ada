import subprocess
import pandas as pd
from collections import defaultdict

def run(command):
    result = subprocess.run(command, stdout=subprocess.PIPE,
            shell=True)
    return result.stdout

def accounts(output):
    lines = output.decode("utf-8").splitlines()
    users = [line.split('|')[0] for line in lines]
    return users

def assocs(output, users):
    lines = output.decode("utf-8").splitlines()
    header, *data = lines
    keys = header.split('|')
    data = list(map(lambda x: x.split('|'), data))
    columns = list(zip(*data))
    _dict = dict(zip(keys, columns))
    df = pd.DataFrame(_dict)
    df = df.loc[df["Account"].isin(users) & df["GrpTRES"]]
    df = df[["Account", "GrpTRES", "GrpTRESMins"]]
    cvit = df[df["Account"] == "cvit"].index
    df.drop(cvit, inplace=True)
    value = lambda x: int(x.split("=")[1])
    df["GrpTRES"] = df["GrpTRES"].apply(value)
    df["GrpTRESMins"] = df["GrpTRESMins"].apply(value)
    return df

def usage(output, users):
    lines = output.decode('utf-8').splitlines()
    assoc_f = lambda s: s.startswith("ClusterName")
    assoc_lines = list(filter(assoc_f, lines))
    
    def parse_assoc(line):
        keyvals = line.split(" ")
        pairs = list(map(lambda x: x.split("=", 1), keyvals))
        d = dict(pairs)
        required = ['Account', 'GrpTRESMins', 'GrpTRES']
        new_d = {key: d[key] for key in required}
        return new_d
    
    d = defaultdict(list)
    for line in assoc_lines:
        nd = parse_assoc(line)
        for key in nd:
            d[key].append(nd[key])
            
    
    def parse_tresmins(s):
        tres = s.split(",")
        d = {}
        def getgpu(tres):
            for keyval in tres:
                key, val = keyval.split('=')
                if key == "gres/gpu":
                    return val
        val = getgpu(tres)
        limit, used = val.split('(')
        used = used.replace(')', '')
        return used
            
    df = pd.DataFrame(d)
    # Process DataFrame for only required values
    new_df = pd.DataFrame()
    new_df["Account"] = df["Account"]
    new_df["GPUUsage"] = df["GrpTRESMins"].astype("str").apply(parse_tresmins)
    new_df = new_df.loc[df["Account"].isin(users) & df["GrpTRES"]]
    cvit = df[df["Account"] == "cvit"].index
    new_df = new_df.drop(cvit)
    
    return new_df


saccounts = run("sacctmgr show accounts where Organization=cvit -p")
users = accounts(saccounts)
sassocs = run("sacctmgr show assoc -p")
meta = assocs(sassocs, users)
susage = run("scontrol show assoc_mgr flags=assoc -o")
usages = usage(susage, users)
final = meta.set_index('Account').join(usages.set_index('Account'), lsuffix="_meta", rsuffix="_usages")

from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument("--output", help="output file", required=True, type=str)
args = parser.parse_args()
final.to_csv(args.output)
