from pymilvus import connections
from utils import *

connections.connect()

get_collections()

load_and_search()

create_index()

load_and_search()
