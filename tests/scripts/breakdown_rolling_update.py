import psutil
import time
from loguru import logger


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='config for rolling update process')
    parser.add_argument('--wait_time', type=int, default=60, help='wait time after rolling update started')
    args = parser.parse_args()
    wait_time = args.wait_time
    logger.info("start to watch rolling update process")
    start_time = time.time()
    end_time = time.time()
    flag = True
    while flag and end_time - start_time < 360: 
        process_list = [p.info for p in psutil.process_iter(attrs=['pid', 'name','cmdline'])]
        for process in process_list:
            if isinstance(process.get("cmdline", []), list):
                if "rollingUpdate.sh" in process.get("cmdline", []):
                    logger.info(f"rolling update process: {process} started")
                    flag = False
                    break
        time.sleep(0.5)
        end_time = time.time()
        if flag:
            logger.info(f"rolling update process not found, wait for {end_time - start_time} seconds")
    logger.info(f"wait {wait_time}s to kill rolling update process")
    time.sleep(wait_time)
    logger.info("start to kill rolling update process")
    try:
        p = psutil.Process(process["pid"])
        p.terminate()
        logger.info(f"rolling update process: {process} killed")
    except Exception as e:
        logger.error(f"rolling update process: {process} kill failed, {e}")

