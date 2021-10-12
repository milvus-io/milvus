import logging
import random
import pdb
import gevent
# import gevent.monkey
# gevent.monkey.patch_all()
from locust import User, between, events, stats
from locust.env import Environment
import locust.stats
import math
from locust import LoadTestShape
from locust.stats import stats_printer, print_stats
from locust.log import setup_logging, greenlet_exception_logger
from milvus_benchmark.client import MilvusClient
from .locust_task import MilvusTask
from .locust_tasks import Tasks
from . import utils

locust.stats.CONSOLE_STATS_INTERVAL_SEC = 20
logger = logging.getLogger("milvus_benchmark.runners.locust_user")
nq = 10000
nb = 100000


class StepLoadShape(LoadTestShape):
    """
    A step load shape
    Keyword arguments:
        step_time -- Time between steps
        step_load -- User increase amount at each step
        spawn_rate -- Users to stop/start per second at every step
        time_limit -- Time limit in seconds
    """

    def init(self, step_time, step_load, spawn_rate, time_limit):
        self.step_time = step_time
        self.step_load = step_load
        self.spawn_rate = spawn_rate
        self.time_limit = time_limit

    def tick(self):
        run_time = self.get_run_time()

        if run_time > self.time_limit:
            return None

        current_step = math.floor(run_time / self.step_time) + 1
        return (current_step * self.step_load, self.spawn_rate)


class MyUser(User):
    # task_set = None
    # wait_time = between(0.001, 0.002)
    pass


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
    MyUser.values = {
        "ids": [random.randint(1000000, 10000000) for _ in range(nb)],
        "get_ids": [random.randint(1, 10000000) for _ in range(nb)],
        "X": utils.generate_vectors(nq, MyUser.op_info["dimension"])
    }

    # MyUser.tasks = {Tasks.query: 1, Tasks.flush: 1}
    MyUser.client = MilvusTask(host=host, port=port, collection_name=collection_name, connection_type=connection_type,
                               m=m)
    if "load_shape" in run_params and run_params["load_shape"]:
        test = StepLoadShape() 
        test.init(run_params["step_time"], run_params["step_load"], run_params["spawn_rate"], run_params["during_time"])
        env = Environment(events=events, user_classes=[MyUser], shape_class=test)
        runner = env.create_local_runner()
        env.runner.start_shape()
    else:
        env = Environment(events=events, user_classes=[MyUser])
        runner = env.create_local_runner()
    # setup logging
    # setup_logging("WARNING", "/dev/null")
    # greenlet_exception_logger(logger=logger)
    gevent.spawn(stats_printer(env.stats))
    # env.create_web_ui("127.0.0.1", 8089)
    # gevent.spawn(stats_printer(env.stats), env, "test", full_history=True)
    # events.init.fire(environment=env, runner=runner)
    clients_num = run_params["clients_num"] if "clients_num" in run_params else 0
    step_load = run_params["step_load"] if "step_load" in run_params else 0
    step_time = run_params["step_time"] if "step_time" in run_params else 0
    spawn_rate = run_params["spawn_rate"]
    during_time = run_params["during_time"]
    runner.start(clients_num, spawn_rate=spawn_rate)
    gevent.spawn_later(during_time, lambda: runner.quit())
    runner.greenlet.join()
    print_stats(env.stats)
    result = {
        "rps": round(env.stats.total.current_rps, 1),  # Number of interface requests per second
        "fail_ratio": env.stats.total.fail_ratio,  # Interface request failure rate
        "max_response_time": round(env.stats.total.max_response_time, 1),  # Maximum interface response time
        "avg_response_time": round(env.stats.total.avg_response_time, 1)  # ratio of average response time
    }
    runner.stop()
    return result
