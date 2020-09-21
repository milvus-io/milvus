import utils

class Const(object):
    # class ConstError(TypeException): pass
    def __setattr__(self, key, value):
        if (key in self.__dict__):
            raise(self.ConstError, "Changing const.%s" % key)
        else:
            self.__dict__[key] = value

    def __getattr__(self, key):
        if (key in self.__dict__):
            return self.key
        else:
            return None

###############################################################
# Constants Definitions
###############################################################
const = Const()

const.default_fields = utils.gen_default_fields()
const.default_binary_fields = utils.gen_binary_default_fields()

const.default_entity = utils.gen_entities(1)
const.default_raw_binary_vector, const.default_binary_entity = utils.gen_binary_entities(1)

const.default_entities = utils.gen_entities(utils.default_nb)
const.default_raw_binary_vectors, const.default_binary_entities = utils.gen_binary_entities(utils.default_nb)
