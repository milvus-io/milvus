import const
import utils

const.port = 19530
const.epsilon = 0.000001

const.default_flush_interval = 1
const.big_flush_interval = 1000

const.default_drop_interval = 3

const.default_dim = 128

const.default_nb = 1000

const.default_top_k = 10

const.default_nlist = 128

const.default_segment_row_limit = 1000
const.default_server_segment_row_limit = 1024 * 512

const.default_partition_name = "_default"

const.default_tag = "1970_01_01"

const.default_float_vec_field_name = "float_vector"
const.default_binary_vec_field_name = "binary_vector"

const.default_fields = utils.gen_default_fields()
const.default_binary_fields = utils.gen_binary_default_fields()

const.default_entity = utils.gen_entities(1)
const.default_raw_binary_vector, const.default_binary_entity = utils.gen_binary_entities(1)

const.default_entities = utils.gen_entities(1200)
const.default_raw_binary_vectors, const.default_binary_entities = utils.gen_binary_entities(1200)
