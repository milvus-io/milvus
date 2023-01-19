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
    flag = False
    while not flag and end_time - start_time < 360: 
        process_list = [p.info for p in psutil.process_iter(attrs=['pid', 'name','cmdline'])]
        for process in process_list:
            logger.debug(process)
            logger.debug("##"*30)
        for process in process_list:
            if isinstance(process.get("cmdline", []), list):
                cmdline_list = process.get("cmdline", [])
                for cmdline in cmdline_list:
                    if "rollingUpdate.sh" in cmdline:
                        logger.info(f"rolling update process: {process} started")
                        flag = True
                        break
            if flag:
                break
        time.sleep(0.5)
        end_time = time.time()
        if not flag:
            logger.info(f"rolling update process not found, wait for {end_time - start_time} seconds")
        else:
            logger.info(f"rolling update process {process} found, wait for {end_time - start_time} seconds")
    if flag:
        logger.info(f"wait {wait_time}s to kill rolling update process")
        time.sleep(wait_time)
        logger.info("start to kill rolling update process")
        try:
            p = psutil.Process(process["pid"])
            p.terminate()
            logger.info(f"rolling update process: {process} killed")
        except Exception as e:
            logger.error(f"rolling update process: {process} kill failed, {e}")
    else:
        logger.info("all process info")
        for process in process_list:
            logger.info(process)
            
            

