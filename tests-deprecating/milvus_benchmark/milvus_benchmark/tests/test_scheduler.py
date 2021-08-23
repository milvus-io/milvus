from milvus_benchmark import back_scheduler


def job_runner():
	print("job_runner")



for i in range(30):
	back_scheduler.add_job(job_runner, args=[], misfire_grace_time=300)
back_scheduler.start()