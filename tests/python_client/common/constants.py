# constants for old pymilvus API test

import utils.util_pymilvus as utils

default_fields = utils.gen_default_fields()
default_binary_fields = utils.gen_binary_default_fields()

default_entity = utils.gen_entities(1)
default_raw_binary_vector, default_binary_entity = utils.gen_binary_entities(1)

default_entity_row = utils.gen_entities_rows(1)
default_raw_binary_vector_row, default_binary_entity_row = utils.gen_binary_entities_rows(1)

default_entities = utils.gen_entities(utils.default_nb)
default_raw_binary_vectors, default_binary_entities = utils.gen_binary_entities(utils.default_nb)

default_entities_new = utils.gen_entities_new(utils.default_nb)
default_raw_binary_vectors_new, default_binary_entities_new = utils.gen_binary_entities_new(utils.default_nb)

default_entities_rows = utils.gen_entities_rows(utils.default_nb)
default_raw_binary_vectors_rows, default_binary_entities_rows = utils.gen_binary_entities_rows(utils.default_nb)
