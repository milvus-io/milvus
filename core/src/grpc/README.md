We manually change two APIs in "milvus.pb.h":
    add_vector_data()
    add_row_id_array()
    add_ids()
    add_distances()
If proto files need be generated again, remember to re-change above APIs.