We manually change two APIs in "milvus.pd.h":
    add_vector_data()
    add_row_id_array()
If proto files need be generated again, remember to re-change above APIs.