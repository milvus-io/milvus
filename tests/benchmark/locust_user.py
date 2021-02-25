import logging
import random
import pdb
import gevent
import gevent.monkey
gevent.monkey.patch_all()

from locust import User, between, events, stats
from locust.env import Environment
import locust.stats
from locust.stats import stats_printer, print_stats

locust.stats.CONSOLE_STATS_INTERVAL_SEC = 30
from locust.log import setup_logging, greenlet_exception_logger

from locust_tasks import Tasks
from client import MilvusClient
from locust_task import MilvusTask

logger = logging.getLogger("__locust__")

class MyUser(User):
    # task_set = None
    wait_time = between(0.001, 0.002)


def locust_executor(host, port, collection_name, connection_type="single", run_params=None):
    m = MilvusClient(host=host, port=port, collection_name=collection_name)
    MyUser.tasks = {}
    tasks = run_params["tasks"]
    for op, weight in tasks.items():
        task = {eval("Tasks."+op): weight}
        MyUser.tasks.update(task)
    logger.error(MyUser.tasks)
    # MyUser.tasks = {Tasks.query: 1, Tasks.flush: 1}
    MyUser.client = MilvusTask(host=host, port=port, collection_name=collection_name, connection_type=connection_type, m=m)
    env = Environment(events=events, user_classes=[MyUser])
    runner = env.create_local_runner()
    # setup logging
    # setup_logging("WARNING", "/dev/null")
    setup_logging("WARNING", "/dev/null")
    greenlet_exception_logger(logger=logger)
    gevent.spawn(stats_printer(env.stats))
    # env.create_web_ui("127.0.0.1", 8089)
    # gevent.spawn(stats_printer(env.stats), env, "test", full_history=True)
    # events.init.fire(environment=env, runner=runner)
    clients_num = run_params["clients_num"]
    spawn_rate = run_params["spawn_rate"]
    during_time = run_params["during_time"]
    runner.start(clients_num, spawn_rate=spawn_rate)
    gevent.spawn_later(during_time, lambda: runner.quit())
    runner.greenlet.join()
    print_stats(env.stats)
    result = {
        "rps": round(env.stats.total.current_rps, 1),
        "fail_ratio": env.stats.total.fail_ratio,
        "max_response_time": round(env.stats.total.max_response_time, 1),
        "min_response_time": round(env.stats.total.avg_response_time, 1)
    }
    runner.stop()
    return result


if __name__ == '__main__':
    connection_type = "single"
    host = "192.168.1.112"
    port = 19530
    collection_name = "sift_1m_2000000_128_l2_2"
    run_params = {"tasks": {"query": 1, "flush": 1}, "clients_num": 1, "spawn_rate": 1, "during_time": 3}
    locust_executor(host, port, collection_name, run_params=run_params)
