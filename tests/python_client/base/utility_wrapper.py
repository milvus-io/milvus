from datetime import datetime
import time
from pymilvus import utility
import sys

sys.path.append("..")
from check.func_check import ResponseChecker
from utils.api_request import api_request
from pymilvus import BulkInsertState
from pymilvus.orm.role import Role
from utils.util_log import test_log as log

TIMEOUT = 20


class ApiUtilityWrapper:
    """ Method of encapsulating utility files """

    ut = utility
    role = None

    def do_bulk_insert(self, collection_name, files="", partition_name=None, timeout=None,
                       using="default", check_task=None, check_items=None, **kwargs):
        working_tasks = self.get_bulk_insert_working_list()
        log.info(f"before bulk load, there are {len(working_tasks)} working tasks")
        log.info(f"files to load: {files}")
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.do_bulk_insert, collection_name,
                                    files, partition_name, timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, using=using).run()
        time.sleep(1)
        working_tasks = self.get_bulk_insert_working_list()
        log.info(f"after bulk load, there are {len(working_tasks)} working tasks")
        return res, check_result

    def get_bulk_insert_state(self, task_id, timeout=None, using="default", check_task=None, check_items=None,
                              **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.get_bulk_insert_state, task_id, timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       task_id=task_id, using=using).run()
        return res, check_result

    def list_bulk_insert_tasks(self, limit=0, collection_name=None, timeout=None, using="default", check_task=None,
                               check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_bulk_insert_tasks, limit, collection_name, timeout, using], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       limit=limit, collection_name=collection_name, using=using).run()
        return res, check_result

    def get_bulk_insert_pending_list(self):
        tasks = {}
        for task in self.ut.list_bulk_insert_tasks():
            if task.state == BulkInsertState.ImportPending:
                tasks[task.task_id] = task
        return tasks

    def get_bulk_insert_working_list(self):
        tasks = {}
        for task in self.ut.list_bulk_insert_tasks():
            if task.state in [BulkInsertState.ImportStarted]:
                tasks[task.task_id] = task
        return tasks

    def list_all_bulk_insert_tasks(self, limit=0):
        tasks, _ = self.list_bulk_insert_tasks(limit=limit)
        pending = 0
        started = 0
        persisted = 0
        completed = 0
        failed = 0
        failed_and_cleaned = 0
        unknown = 0
        for task in tasks:
            print(task)
            if task.state == BulkInsertState.ImportPending:
                pending = pending + 1
            elif task.state == BulkInsertState.ImportStarted:
                started = started + 1
            elif task.state == BulkInsertState.ImportPersisted:
                persisted = persisted + 1
            elif task.state == BulkInsertState.ImportCompleted:
                completed = completed + 1
            elif task.state == BulkInsertState.ImportFailed:
                failed = failed + 1
            elif task.state == BulkInsertState.ImportFailedAndCleaned:
                failed_and_cleaned = failed_and_cleaned + 1
            else:
                unknown = unknown + 1

        log.info("There are", len(tasks), "bulkload tasks.", pending, "pending,", started, "started,", persisted,
                 "persisted,", completed, "completed,", failed, "failed", failed_and_cleaned, "failed_and_cleaned",
                 unknown, "unknown")

    def wait_for_bulk_insert_tasks_completed(self, task_ids, target_state=BulkInsertState.ImportCompleted,
                                           timeout=None, using="default", **kwargs):
        start = time.time()
        tasks_state_distribution = {
            "success": set(),
            "failed": set(),
            "in_progress": set()
        }
        tasks_state = {}
        if timeout is not None:
            task_timeout = timeout
        else:
            task_timeout = TIMEOUT
        start = time.time()
        end = time.time()
        log.info(f"wait bulk load timeout is {task_timeout}")
        pending_tasks = self.get_bulk_insert_pending_list()
        log.info(f"before waiting, there are {len(pending_tasks)} pending tasks")
        while len(tasks_state_distribution["success"]) + len(tasks_state_distribution["failed"]) < len(
                task_ids) and end - start <= task_timeout:
            time.sleep(2)

            for task_id in task_ids:
                if task_id in tasks_state_distribution["success"] or task_id in tasks_state_distribution["failed"]:
                    continue
                else:
                    state, _ = self.get_bulk_insert_state(task_id, task_timeout, using, **kwargs)
                    tasks_state[task_id] = state

                    if target_state == BulkInsertState.ImportPersisted:
                        if state.state in [BulkInsertState.ImportPersisted, BulkInsertState.ImportCompleted]:
                            if task_id in tasks_state_distribution["in_progress"]:
                                tasks_state_distribution["in_progress"].remove(task_id)
                            tasks_state_distribution["success"].add(task_id)
                        elif state.state in [BulkInsertState.ImportPending, BulkInsertState.ImportStarted]:
                            tasks_state_distribution["in_progress"].add(task_id)
                        else:
                            tasks_state_distribution["failed"].add(task_id)

                    if target_state == BulkInsertState.ImportCompleted:
                        if state.state in [BulkInsertState.ImportCompleted]:
                            if task_id in tasks_state_distribution["in_progress"]:
                                tasks_state_distribution["in_progress"].remove(task_id)
                            tasks_state_distribution["success"].add(task_id)
                        elif state.state in [BulkInsertState.ImportPending, BulkInsertState.ImportStarted,
                                             BulkInsertState.ImportPersisted]:
                            tasks_state_distribution["in_progress"].add(task_id)
                        else:
                            tasks_state_distribution["failed"].add(task_id)

            end = time.time()
        pending_tasks = self.get_bulk_insert_pending_list()
        log.info(f"after waiting, there are {len(pending_tasks)} pending tasks")
        log.info(f"task state distribution: {tasks_state_distribution}")
        log.info(tasks_state)
        if len(tasks_state_distribution["success"]) == len(task_ids):
            log.info(f"wait for bulk load tasks completed successfully, cost time: {end - start}")
            return True, tasks_state
        else:
            log.info(f"wait for bulk load tasks completed failed, cost time: {end - start}")
            return False, tasks_state

    def wait_all_pending_tasks_finished(self):
        task_states_map = {}
        all_tasks, _ = self.list_bulk_insert_tasks()
        # log.info(f"all tasks: {all_tasks}")
        for task in all_tasks:
            if task.state in [BulkInsertState.ImportStarted, BulkInsertState.ImportPersisted]:
                task_states_map[task.task_id] = task.state

        log.info(f"current tasks states: {task_states_map}")
        pending_tasks = self.get_bulk_insert_pending_list()
        working_tasks = self.get_bulk_insert_working_list()
        log.info(
            f"in the start, there are {len(working_tasks)} working tasks, {working_tasks} {len(pending_tasks)} pending tasks, {pending_tasks}")
        time_cnt = 0
        pending_task_ids = set()
        while len(pending_tasks) > 0:
            time.sleep(5)
            time_cnt += 5
            pending_tasks = self.get_bulk_insert_pending_list()
            working_tasks = self.get_bulk_insert_working_list()
            cur_pending_task_ids = []
            for task_id in pending_tasks.keys():
                cur_pending_task_ids.append(task_id)
                pending_task_ids.add(task_id)
            log.info(
                f"after {time_cnt}, there are {len(working_tasks)} working tasks, {len(pending_tasks)} pending tasks")
            log.debug(f"total pending tasks: {pending_task_ids} current pending tasks: {cur_pending_task_ids}")
        log.info(f"after {time_cnt}, all pending tasks are finished")
        all_tasks, _ = self.list_bulk_insert_tasks()
        for task in all_tasks:
            if task.task_id in pending_task_ids:
                log.info(f"task {task.task_id} state transfer from pending to {task.state_name}")

    def get_query_segment_info(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.get_query_segment_info, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, timeout=timeout, using=using).run()
        return res, check_result

    def loading_progress(self, collection_name, partition_names=None,
                         using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.loading_progress, collection_name, partition_names, using])
        check_result = ResponseChecker(res, func_name, check_task,
                                       check_items, is_succ, collection_name=collection_name,
                                       partition_names=partition_names, using=using).run()
        return res, check_result

    def wait_for_loading_complete(self, collection_name, partition_names=None, timeout=None, using="default",
                                  check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.wait_for_loading_complete, collection_name,
                                    partition_names, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, partition_names=partition_names,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def index_building_progress(self, collection_name, index_name="", using="default",
                                check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.index_building_progress, collection_name, index_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, index_name=index_name,
                                       using=using).run()
        return res, check_result

    def wait_for_index_building_complete(self, collection_name, index_name="", timeout=None, using="default",
                                         check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.wait_for_index_building_complete, collection_name,
                                    index_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, index_name=index_name,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def has_collection(self, collection_name, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.has_collection, collection_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name, using=using).run()
        return res, check_result

    def has_partition(self, collection_name, partition_name, using="default",
                      check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.has_partition, collection_name, partition_name, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name,
                                       partition_name=partition_name, using=using).run()
        return res, check_result

    def drop_collection(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.drop_collection, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       collection_name=collection_name,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def list_collections(self, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_collections, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def calc_distance(self, vectors_left, vectors_right, params=None, timeout=None,
                      using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.calc_distance, vectors_left, vectors_right,
                                    params, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def load_balance(self, collection_name, src_node_id, dst_node_ids, sealed_segment_ids, timeout=None,
                     using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout

        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.load_balance, collection_name, src_node_id, dst_node_ids,
                                    sealed_segment_ids, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def create_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.create_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def drop_alias(self, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.drop_alias, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def alter_alias(self, collection_name, alias, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.alter_alias, collection_name, alias, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def list_aliases(self, collection_name, timeout=None, using="default", check_task=None, check_items=None):
        timeout = TIMEOUT if timeout is None else timeout
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_aliases, collection_name, timeout, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       timeout=timeout, using=using).run()
        return res, check_result

    def mkts_from_datetime(self, d_time=None, milliseconds=0., delta=None):
        d_time = datetime.now() if d_time is None else d_time
        res, _ = api_request([self.ut.mkts_from_datetime, d_time, milliseconds, delta])
        return res

    def mkts_from_hybridts(self, hybridts, milliseconds=0., delta=None):
        res, _ = api_request([self.ut.mkts_from_hybridts, hybridts, milliseconds, delta])
        return res

    def create_user(self, user, password, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.create_user, user, password, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, using=using).run()
        return res, check_result

    def list_usernames(self, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_usernames, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       using=using).run()
        return res, check_result

    def reset_password(self, user, old_password, new_password, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.reset_password, user, old_password, new_password])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ).run()
        return res, check_result

    def update_password(self, user, old_password, new_password, check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.update_password, user, old_password, new_password])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ).run()
        return res, check_result

    def delete_user(self, user, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.delete_user, user, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       using=using).run()
        return res, check_result

    def list_roles(self, include_user_info: bool, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_roles, include_user_info, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, using=using).run()
        return res, check_result

    def list_user(self, username: str, include_role_info: bool, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_user, username, include_role_info, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, using=using).run()
        return res, check_result

    def list_users(self, include_role_info: bool, using="default", check_task=None, check_items=None):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.ut.list_users, include_role_info, using])
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ, using=using).run()
        return res, check_result

    def init_role(self, name, using="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([Role, name, using], **kwargs)
        self.role = res if is_succ else None
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       name=name, **kwargs).run()
        return res, check_result

    def create_role(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, is_succ = api_request([self.role.create], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, is_succ,
                                       **kwargs).run()
        return res, check_result

    def role_drop(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.drop], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_is_exist(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.is_exist], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_add_user(self, username: str, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.add_user, username], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_remove_user(self, username: str, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.remove_user, username], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_get_users(self, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.get_users], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    @property
    def role_name(self):
        return self.role.name

    def role_grant(self, object: str, object_name: str, privilege: str, db_name="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.grant, object, object_name, privilege, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_revoke(self, object: str, object_name: str, privilege: str, db_name="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.revoke, object, object_name, privilege, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_list_grant(self, object: str, object_name: str, db_name="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.list_grant, object, object_name, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def role_list_grants(self, db_name="default", check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.role.list_grants, db_name], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def create_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.create_resource_group, name, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def drop_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.drop_resource_group, name, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def list_resource_groups(self, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.list_resource_groups, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def describe_resource_group(self, name, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.describe_resource_group, name, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def transfer_node(self, source, target, num_node, using="default", timeout=None, check_task=None, check_items=None,
                      **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.transfer_node, source, target, num_node, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def transfer_replica(self, source, target, collection_name, num_replica, using="default", timeout=None,
                         check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request(
            [self.ut.transfer_replica, source, target, collection_name, num_replica, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check, **kwargs).run()
        return res, check_result

    def rename_collection(self, old_collection_name, new_collection_name, timeout=None, check_task=None,
                          check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.rename_collection, old_collection_name, new_collection_name, timeout],
                                 **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       old_collection_name=old_collection_name, new_collection_name=new_collection_name,
                                       timeout=timeout, **kwargs).run()
        return res, check_result

    def flush_all(self, using="default", timeout=None, check_task=None, check_items=None, **kwargs):
        func_name = sys._getframe().f_code.co_name
        res, check = api_request([self.ut.flush_all, using, timeout], **kwargs)
        check_result = ResponseChecker(res, func_name, check_task, check_items, check,
                                       using=using, timeout=timeout, **kwargs).run()
        return res, check_result

