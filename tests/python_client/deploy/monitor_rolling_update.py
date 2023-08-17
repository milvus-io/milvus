import argparse
import subprocess
import time
from loguru import logger as log


def run_kubectl_get_pod(duration, interval, release_name):
    end_time = time.time() + duration
    while time.time() < end_time:
        cmd = f"kubectl get pod |grep {release_name}"
        res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = res.communicate()
        output = stdout.decode("utf-8")
        log.info(f"{cmd}\n{output}\n")
        time.sleep(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to run "kubectl get pod" command at regular intervals')
    parser.add_argument('-d', '--duration', type=int, default=600, help='Duration in seconds (default: 600)')
    parser.add_argument('-i', '--interval', type=int, default=5, help='Interval in seconds (default: 30)')
    parser.add_argument('-n', '--release_name', type=str, default="", help='release name (default: "None")')
    args = parser.parse_args()

    run_kubectl_get_pod(args.duration, args.interval, args.release_name)


