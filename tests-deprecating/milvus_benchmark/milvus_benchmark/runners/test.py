import math
from locust import User, TaskSet, task, constant
from locust import LoadTestShape


class StepLoadShape(LoadTestShape):
    """
    A step load shape
    Keyword arguments:
        step_time -- Time between steps
        step_load -- User increase amount at each step
        spawn_rate -- Users to stop/start per second at every step
        time_limit -- Time limit in seconds
    """

    step_time = 30
    step_load = 10
    spawn_rate = 10
    time_limit = 600

    def tick(self):
        run_time = self.get_run_time()

        if run_time > self.time_limit:
            return None

        current_step = math.floor(run_time / self.step_time) + 1
        return (current_step * self.step_load, self.spawn_rate)


class UserTasks(TaskSet):
    @task
    def get_root(self):
        print("in usertasks")


class WebsiteUser(User):
    wait_time = constant(0.5)
    tasks = [UserTasks]
    shape = StepLoadShape
