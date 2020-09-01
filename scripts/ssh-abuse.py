import subprocess as sp
import random
import time
import json
import os
import sys
from argparse import ArgumentParser
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5.5s]  %(message)s"
)
logger = logging.getLogger()


class State:
    def __init__(self, fname):
        self.fname = fname
        self.load()
        self.counts = {}

    def load(self):
        if os.path.exists(self.fname):
            with open(self.fname, 'r+') as fp:
                serialized = json.load(fp)
                self.actions = serialized['actions']
                self.counts = serialized['counts']
        else:
            self.actions = []

    def save(self):
        serialized = {
            "actions": self.actions,
            "counts": self.counts
        }

    def issue_kill(self, account, jobId, nodeList):
        cmd = 'scancel -s SIGHUP {} && sleep 30m && scancel -s SIGTERM {}'.format(jobId, jobId)
        if cmd not in self.actions:
            # handle
            logging.info(cmd)

            if account not in self.counts:
                self.counts[account] = 0

            self.counts[account] += 1
            self.actions.append(cmd)

            self.save()

            # The following is critical, handle with care.
            # sp.Popen(cmd, shell=True)



def run(cmd):
    output = sp.check_output(cmd, shell=True).decode("utf-8")
    return output

def execute(state, line, random_threshold):
    account, jobId, nodeList = line.split(',')
    if random.random() < random_threshold:
        logger.info(line)
        state.issue_kill(account, jobId, nodeList)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--store', type=str, required=True)
    parser.add_argument('--user-list', type=str, required=True)
    parser.add_argument('--threshold', type=float, required=True)
    parser.add_argument('--poll-window', type=int, required=True)
    args = parser.parse_args()
    state = State(args.store)
    duration = 60 * args.poll_window
    cmd = 'squeue -A {} -h --format=%a,%A,%n'.format(args.user_list)
    while True:
        output = run(cmd)
        lines = list(filter(lambda x: x, output.splitlines()))
        for line in lines:
            execute(state, line, args.threshold)
        time.sleep(duration)

