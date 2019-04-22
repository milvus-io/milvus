#! /usr/bin/env python
import subprocess
import argparse
import random
import time
import sys


def generate_runtimes(total_runtime):
    # combination of short runtimes and long runtimes, with heavier
    # weight on short runtimes
    possible_runtimes_sec = range(1, 10) + range(1, 20) + [100, 1000]
    runtimes = []
    while total_runtime > 0:
        chosen = random.choice(possible_runtimes_sec)
        chosen = min(chosen, total_runtime)
        runtimes.append(chosen)
        total_runtime -= chosen
    return runtimes


def main(args):
    runtimes = generate_runtimes(int(args.runtime_sec))
    print "Going to execute write stress for " + str(runtimes)  # noqa: E999 T25377293 Grandfathered in
    first_time = True

    for runtime in runtimes:
        kill = random.choice([False, True])

        cmd = './write_stress --runtime_sec=' + \
            ("-1" if kill else str(runtime))

        if len(args.db) > 0:
            cmd = cmd + ' --db=' + args.db

        if first_time:
            first_time = False
        else:
            # use current db
            cmd = cmd + ' --destroy_db=false'
        if random.choice([False, True]):
            cmd = cmd + ' --delete_obsolete_files_with_fullscan=true'
        if random.choice([False, True]):
            cmd = cmd + ' --low_open_files_mode=true'

        print("Running write_stress for %d seconds (%s): %s" %
              (runtime, ("kill-mode" if kill else "clean-shutdown-mode"),
              cmd))

        child = subprocess.Popen([cmd], shell=True)
        killtime = time.time() + runtime
        while not kill or time.time() < killtime:
            time.sleep(1)
            if child.poll() is not None:
                if child.returncode == 0:
                    break
                else:
                    print("ERROR: write_stress died with exitcode=%d\n"
                          % child.returncode)
                    sys.exit(1)
        if kill:
            child.kill()
        # breathe
        time.sleep(3)

if __name__ == '__main__':
    random.seed(time.time())
    parser = argparse.ArgumentParser(description="This script runs and kills \
        write_stress multiple times")
    parser.add_argument("--runtime_sec", default='1000')
    parser.add_argument("--db", default='')
    args = parser.parse_args()
    main(args)
