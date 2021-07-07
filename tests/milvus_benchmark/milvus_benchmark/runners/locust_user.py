import logging
import random
import pdb
import gevent
# import gevent.monkey
# gevent.monkey.patch_all()
from locust import Locust, User, TaskSet, task, between, events, stats
from locust.env import Environment
import locust.stats
from locust.stats import stats_printer, print_stats
from locust.log import setup_logging, greenlet_exception_logger
from milvus_benchmark.client import MilvusClient
from .locust_task import MilvusTask
from .locust_tasks import Tasks

locust.stats.CONSOLE_STATS_INTERVAL_SEC = 30
logger = logging.getLogger("milvus_benchmark.runners.locust_user")


class MyUser(User):
    # task_set = None
    wait_time = between(0.001, 0.002)


def locust_executor(host, port, collection_name, connection_type="single", run_params=None):
    m = MilvusClient(host=host, port=port, collection_name=collection_name)
    MyUser.tasks = {}
    MyUser.op_info = run_params["op_info"]
    MyUser.params = {}
    tasks = run_params["tasks"]
    for op, value in tasks.items():
        task = {eval("Tasks." + op): value["weight"]}
        MyUser.tasks.update(task)
        MyUser.params[op] = value["params"] if "params" in value else None
    logger.info(MyUser.tasks)

    MyUser.tasks = {Tasks.load: 1, Tasks.flush: 1}
    MyUser.client = MilvusTask(host=host, port=port, collection_name=collection_name, connection_type=connection_type,
                               m=m)
    # MyUser.info = m.get_info(collection_name)
    env = Environment(events=events, user_classes=[MyUser])

    runner = env.create_local_runner()
    # setup logging
    # setup_logging("WARNING", "/dev/null")
    # greenlet_exception_logger(logger=logger)
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
        "avg_response_time": round(env.stats.total.avg_response_time, 1)
    }
    runner.stop()
    return result
