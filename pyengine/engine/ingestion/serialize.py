import faiss

def write_index(index, file_name):
    faiss.write_index(index, file_name)

def read_index(file_name):
    return faiss.read_index(file_name)