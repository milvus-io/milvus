# import docker
from pymilvus import connections
from utils import *

connections.connect()

get_collections()

load_and_search()

create_collections_and_insert_data()
