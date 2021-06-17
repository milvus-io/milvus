import os
import threading
import glob
from delayed_assert import expect
import constants
from yaml import full_load


def check_config(chaos_config):
    if not chaos_config.get('kind', None):
        raise Exception("kind is must be specified")
    if not chaos_config.get('spec', None):
        raise Exception("spec is must be specified")
    if "action" not in chaos_config.get('spec', None):
        raise Exception("action is must be specified in spec")
    if "selector" not in chaos_config.get('spec', None):
        raise Exception("selector is must be specified in spec")
    return True


def reset_counting(checkers={}):
    for ch in checkers.values():
        ch.reset()


def gen_experiment_config(yaml):
    with open(yaml) as f:
        _config = full_load(f)
        f.close()
    return _config


def start_monitor_threads(checkers={}):
    for k in checkers.keys():
        v = checkers[k]
        t = threading.Thread(target=v.keep_running, args=())
        t.start()


def assert_statistic(checkers, expectations={}):
    for k in checkers.keys():
        # expect succ if no expectations
        succ_rate = checkers[k].succ_rate()
        if expectations.get(k, '') == constants.FAIL:
            print(f"Expect Fail: {str(checkers[k])} current succ rate {succ_rate}")
            expect(succ_rate < 0.49)
        else:
            print(f"Expect Succ: {str(checkers[k])} current succ rate {succ_rate}")
            expect(succ_rate > 0.90)


def get_env_variable_by_name(name):
    """ get env variable by name"""
    try:
        env_var = os.environ[name]
        print(f"env_variable: {env_var}")
        return str(env_var)
    except Exception as e:
        print(f"fail to get env variables, error: {str(e)}")
        return None


def get_chaos_yamls():
    chaos_env = get_env_variable_by_name(constants.CHAOS_CONFIG_ENV)
    if chaos_env is not None:
        if os.path.isdir(chaos_env):
            print(f"chaos_env is a dir: {chaos_env}")
            return glob.glob(chaos_env + 'chaos_*.yaml')
        elif os.path.isfile(chaos_env):
            print(f"chaos_env is a file: {chaos_env}")
            return [chaos_env]
        else:
            # not a valid directory, return default
            pass
    print("not a valid directory or file, return default")
    return glob.glob(constants.TESTS_CONFIG_LOCATION + 'chaos_*.yaml')

