import os
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--account', help='User name', required=True)
parser.add_argument('--num_gpu', help='Number of gpus', default=1, type=int)
parser.add_argument('--gpu_hours', help='Gpu hours', default=600, type=int)

args = parser.parse_args()

template = 'sacctmgr -i modify account {} set GrpTRES=gres/gpu={} GrpTRESMin=gres/gpu={}'

command = template.format(args.account, args.num_gpu, args.gpu_hours)
print (command)

#subprocess.call(command, shell=True)

print ("Modified the given account")