# def pull_image(image):
#     registry = image.split(":")[0]
#     image_tag = image.split(":")[1]
#     client = docker.APIClient(base_url='unix://var/run/docker.sock')
#     logger.info("Start pulling image: %s" % image)
#     return client.pull(registry, image_tag)


# def run_server(image, mem_limit=None, timeout=30, test_type="local", volume_name=None, db_slave=None):
#     import colors

#     client = docker.from_env()
#     # if mem_limit is None:
#     #     mem_limit = psutil.virtual_memory().available
#     # logger.info('Memory limit:', mem_limit)
#     # cpu_limit = "0-%d" % (multiprocessing.cpu_count() - 1)
#     # logger.info('Running on CPUs:', cpu_limit)
#     for dir_item in ['logs', 'db']:
#         try:
#             os.mkdir(os.path.abspath(dir_item))
#         except Exception as e:
#             pass

#     if test_type == "local":
#         volumes = {
#             os.path.abspath('conf'):
#                 {'bind': '/opt/milvus/conf', 'mode': 'ro'},
#             os.path.abspath('logs'):
#                 {'bind': '/opt/milvus/logs', 'mode': 'rw'},
#             os.path.abspath('db'):
#                 {'bind': '/opt/milvus/db', 'mode': 'rw'},
#         }
#     elif test_type == "remote":
#         if volume_name is None:
#             raise Exception("No volume name")
#         remote_log_dir = volume_name+'/logs'
#         remote_db_dir = volume_name+'/db'

#         for dir_item in [remote_log_dir, remote_db_dir]:
#             if not os.path.isdir(dir_item):
#                 os.makedirs(dir_item, exist_ok=True)
#         volumes = {
#             os.path.abspath('conf'):
#                 {'bind': '/opt/milvus/conf', 'mode': 'ro'},
#             remote_log_dir:
#                 {'bind': '/opt/milvus/logs', 'mode': 'rw'},
#             remote_db_dir:
#                 {'bind': '/opt/milvus/db', 'mode': 'rw'}
#         }
#         # add volumes
#         if db_slave and isinstance(db_slave, int):
#             for i in range(2, db_slave+1):
#                 remote_db_dir = volume_name+'/data'+str(i)
#                 if not os.path.isdir(remote_db_dir):
#                     os.makedirs(remote_db_dir, exist_ok=True)
#                 volumes[remote_db_dir] = {'bind': '/opt/milvus/data'+str(i), 'mode': 'rw'}

#     container = client.containers.run(
#         image,
#         volumes=volumes,
#         runtime="nvidia",
#         ports={'19530/tcp': 19530, '8080/tcp': 8080},
#         # environment=["OMP_NUM_THREADS=48"],
#         # cpuset_cpus=cpu_limit,
#         # mem_limit=mem_limit,
#         # environment=[""],
#         detach=True)

#     def stream_logs():
#         for line in container.logs(stream=True):
#             logger.info(colors.color(line.decode().rstrip(), fg='blue'))

#     if sys.version_info >= (3, 0):
#         t = threading.Thread(target=stream_logs, daemon=True)
#     else:
#         t = threading.Thread(target=stream_logs)
#         t.daemon = True
#     t.start()

#     logger.info('Container: %s started' % container)
#     return container
#     # exit_code = container.wait(timeout=timeout)
#     # # Exit if exit code
#     # if exit_code == 0:
#     #     return container
#     # elif exit_code is not None:
#     #     print(colors.color(container.logs().decode(), fg='red'))

# def restart_server(container):
#     client = docker.APIClient(base_url='unix://var/run/docker.sock')

#     client.restart(container.name)
#     logger.info('Container: %s restarted' % container.name)
#     return container


# def remove_container(container):
#     container.remove(force=True)
#     logger.info('Container: %s removed' % container)


# def remove_all_containers(image):
#     client = docker.from_env()
#     try:
#         for container in client.containers.list():
#             if image in container.image.tags:
#                 container.stop(timeout=30)
#                 container.remove(force=True)
#     except Exception as e:
#         logger.error("Containers removed failed")


# def container_exists(image):
#     '''
#     Check if container existed with the given image name
#     @params: image name
#     @return: container if exists
#     '''
#     res = False
#     client = docker.from_env()
#     for container in client.containers.list():
#         if image in container.image.tags:
#             # True
#             res = container
#     return res

