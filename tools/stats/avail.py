import subprocess as sp
from collections import defaultdict
from pprint import pprint
import humanfriendly

def run(cmd):
    p = sp.check_output(cmd, shell=True)
    return p.decode("utf-8")


class TRES:
    def __init__(self, cpu, mem, gpu):
        self.cpu = cpu
        self.mem = mem
        self.gpu = gpu

    @classmethod
    def build(cls, keyvals):
        state = defaultdict(lambda : '0')
        for keyval in keyvals.split(','):
            if keyval:
                key, val = keyval.split('=')
                state[key] = val

        # print(state)
        cpu = int(state["cpu"])
        mem = humanfriendly.parse_size(state["mem"])
        gpu = int(state["gres/gpu"])

        return cls(cpu, mem, gpu)


    def __repr__(self):
        mem = humanfriendly.format_size(self.mem)
        return 'cpu={},mem={},gres/gpu={}'.format(self.cpu, mem, self.gpu)

    def __sub__(self, other):
        return TRES(self.cpu - other.cpu, self.mem - other.mem, self.gpu-other.gpu)

class Node:
    def __init__(self, NodeName, CfgTRES, AllocTRES):
        self.name = NodeName
        self.cfg = TRES.build(CfgTRES)
        self.alloc = TRES.build(AllocTRES)

    @property
    def free(self):
        return self.cfg - self.alloc
    
    def __repr__(self):
        flag = '[green]' if self.gainable() else '[nopes]'
        return '{}: {}\t{}\t{}\t{}'.format(flag, self.name, self.cfg, self.alloc, self.free)

    @property
    def gainable(self):
        return not (self.free.gpu == 0 or \
                self.free.cpu == 0 or \
                self.free.mem == 0)


        
        

def get_nodes():
    required = ["CfgTRES", "AllocTRES", "NodeName"]
    output = run("scontrol show nodes -o")
    nodes = []
    for line in output.splitlines():
        keyvals = line.split(' ')
        d = {}
        for keyval in keyvals:
            key, *val = keyval.split("=")
            val = '='.join(val)
            if key in required:
                d[key] = val
        if set(d.keys()) == set(required):
            node = Node(**d)
            nodes.append(node)
        else:
            raise
            pass
    return nodes

if __name__ == '__main__':
    nodes = get_nodes()
    nodes = sorted(nodes, key = lambda x: (x.gainable, x.free.gpu, x.free.cpu, x.free.mem), 
            reverse=True)

    print("Gainable nodes")
    gainable_nodes = list(filter(lambda x: x.gainable, nodes))
    for node in gainable_nodes:
        print(node.name, node.free)

    print("Blocked nodes")
    blocked_nodes = list(filter(lambda x: not x.gainable, nodes))
    print('-'*10)
    for node in blocked_nodes:
        print(node.name, node.free)
