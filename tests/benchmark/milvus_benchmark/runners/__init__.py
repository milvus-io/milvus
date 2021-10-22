from .accuracy import AccAccuracyRunner, AccuracyRunner
from .build import BuildRunner, InsertBuildRunner
from .chaos import SimpleChaosRunner
from .get import InsertGetRunner
from .insert import BPInsertRunner, InsertRunner
from .locust import LocustInsertRunner, LocustRandomRunner, LocustSearchRunner
from .search import InsertSearchRunner, SearchRunner


def get_runner(name, env, metric):
    return {
        "insert_performance": InsertRunner(env, metric),
        "bp_insert_performance": BPInsertRunner(env, metric),
        "search_performance": SearchRunner(env, metric),
        "insert_search_performance": InsertSearchRunner(env, metric),
        "locust_insert_performance": LocustInsertRunner(env, metric),
        "locust_search_performance": LocustSearchRunner(env, metric),
        "locust_random_performance": LocustRandomRunner(env, metric),
        "insert_build_performance": InsertBuildRunner(env, metric),
        "insert_get_performance": InsertGetRunner(env, metric),
        "build_performance": BuildRunner(env, metric),
        "accuracy": AccuracyRunner(env, metric),
        "ann_accuracy": AccAccuracyRunner(env, metric),
        "simple_chaos": SimpleChaosRunner(env, metric),
    }.get(name)
