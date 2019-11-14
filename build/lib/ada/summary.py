from collections import defaultdict
import pandas as pd
import humanfriendly

def summarize(jobs_table, nodes_table):
    table = jobs_table
    nt = nodes_table
    accessible = nt.accessible()
    ghost = nt.ghost()
    d = defaultdict(list)
    cumulative = defaultdict(float)
    ratios = {"cpu": 40, "mem": humanfriendly.parse_size('126G'), "gres/gpu": 4}
    for account in ['cvit', 'nlp', 'ccnsb', 'research', 'sub', 'mll']:
        df = table.account(account, remove=False)
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

