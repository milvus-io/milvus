import json
from pprint import pprint
import jenkins

JENKINS_URL = "****"
server = jenkins.Jenkins(JENKINS_URL, username='****', password='****')
user = server.get_whoami()
version = server.get_version()
print('Hello %s from Jenkins %s' % (user['fullName'], version))


# print(job_config)
# build_params = {
#     "SUITE": "gpu_accuracy_ann_debug.yaml",
#     "IMAGE_TYPE": "cpu",
#     "IMAGE_VERSION": "tanimoto_distance",
#     "SERVER_HOST": "eros"
# }
# print(server.build_job(job_name, build_params))


with open("default_config.json") as json_file:
	data = json.load(json_file)
	for config in data:
		build_params = config["build_params"]
		job_name = config["job_name"]
		res = server.build_job(job_name, build_params)
		print(job_name, res)
