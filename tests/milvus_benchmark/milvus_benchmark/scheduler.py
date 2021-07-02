# import logging
# from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.schedulers.blocking import BlockingScheduler

# from apscheduler.jobstores.mongodb import MongoDBJobStore
# from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
# from apscheduler.executors.debug import DebugExecutor
# import config
# from pymongo import MongoClient

# logger = logging.basicConfig()

# mongo_client = MongoClient(config.MONGO_SERVER)
# jobstores = {
#     'default': MongoDBJobStore(database=config.SCHEDULER_DB, collection=config.JOB_COLLECTION, client=mongo_client)
# }

# executors = {
#     'default': ThreadPoolExecutor(max_workers=100)
# }

# job_defaults = {
#     'coalesce': True,
#     'max_instances': 32
# }
# # TODO:
# back_scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults, logger=logger)