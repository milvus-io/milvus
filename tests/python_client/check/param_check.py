import sys
import operator
from common import common_type as ct

sys.path.append("..")
from utils.util_log import test_log as log


def ip_check(ip):
    if ip == "localhost":
        return True

    if not isinstance(ip, str):
        log.error("[IP_CHECK] IP(%s) is not a string." % ip)
        return False

    return True


def number_check(num):
    if str(num).isdigit():
        return True

    else:
        log.error("[NUMBER_CHECK] Number(%s) is not a numbers." % num)
        return False


def exist_check(param, _list):
    if param in _list:
        return True

    else:
        log.error("[EXIST_CHECK] Param(%s) is not in (%s)." % (param, _list))
        return False


def dict_equal_check(dict1, dict2):
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        log.error("[DICT_EQUAL_CHECK] Type of dict(%s) or dict(%s) is not a dict." % (str(dict1), str(dict2)))
        return False
    return operator.eq(dict1, dict2)


def list_de_duplication(_list):
    if not isinstance(_list, list):
        log.error("[LIST_DE_DUPLICATION] Type of list(%s) is not a list." % str(_list))
        return _list

    # de-duplication of _list
    result = list(set(_list))

    # Keep the order of the elements unchanged
    result.sort(key=_list.index)

    log.debug("[LIST_DE_DUPLICATION] %s after removing the duplicate elements, the list becomes %s" % (
        str(_list), str(result)))
    return result


def list_equal_check(param1, param2):
    check_result = True

    if len(param1) == len(param1):
        _list1 = list_de_duplication(param1)
        _list2 = list_de_duplication(param2)

        if len(_list1) == len(_list2):
            for i in _list1:
                if i not in _list2:
                    check_result = False
                    break
        else:
            check_result = False
    else:
        check_result = False

    if check_result is False:
        log.error("[LIST_EQUAL_CHECK] List(%s) and list(%s) are not equal." % (str(param1), str(param2)))

    return check_result


def list_contain_check(sublist, superlist):
    if not isinstance(sublist, list):
        raise Exception("%s isn't list type" % sublist)
    if not isinstance(superlist, list):
        raise Exception("%s isn't list type" % superlist)

    check_result = True
    for i in sublist:
        if i not in superlist:
            check_result = False
            break
        else:
            superlist.remove(i)
    if not check_result:
        log.error("list_contain_check: List(%s) does not contain list(%s)"
                  % (str(superlist), str(sublist)))

    return check_result


def get_connect_object_name(_list):
    """ get the name of the objects that returned by the connection """
    if not isinstance(_list, list):
        log.error("[GET_CONNECT_OBJECT_NAME] Type of list(%s) is not a list." % str(_list))
        return _list

    new_list = []
    for i in _list:
        if not isinstance(i, tuple):
            log.error("[GET_CONNECT_OBJECT_NAME] The element:%s of the list is not tuple, please check manually."
                      % str(i))
            return _list

        if len(i) != 2:
            log.error("[GET_CONNECT_OBJECT_NAME] The length of the tuple:%s is not equal to 2, please check manually."
                      % str(i))
            return _list

        if i[1] is not None:
            _obj_name = type(i[1]).__name__
            new_list.append((i[0], _obj_name))
        else:
            new_list.append(i)

    log.debug("[GET_CONNECT_OBJECT_NAME] list:%s is reset to list:%s" % (str(_list), str(new_list)))
    return new_list


def equal_entity(exp, actual):
    """
    compare two entities containing vector field
    {"int64": 0, "float": 0.0, "float_vec": [0.09111554112502457, ..., 0.08652634258062468]}
    :param exp: exp entity
    :param actual: actual entity
    :return: bool
    """
    assert actual.keys() == exp.keys()
    for field, value in exp.items():
        if isinstance(value, list):
            assert len(actual[field]) == len(exp[field])
            for i in range(0, len(exp[field]), 4):
                assert abs(actual[field][i] - exp[field][i]) < ct.epsilon
        else:
            assert actual[field] == exp[field]
    return True


def entity_in(entity, entities, primary_field):
    """
    according to the primary key to judge entity in the entities list
    :param entity: dict
            {"int": 0, "vec": [0.999999, 0.111111]}
    :param entities: list of dict
            [{"int": 0, "vec": [0.999999, 0.111111]}, {"int": 1, "vec": [0.888888, 0.222222]}]
    :param primary_field: collection primary field
    :return: True or False
    """
    primary_default = ct.default_int64_field_name
    primary_field = primary_default if primary_field is None else primary_field
    primary_key = entity.get(primary_field, None)
    primary_keys = []
    for e in entities:
        primary_keys.append(e[primary_field])
    if primary_key not in primary_keys:
        return False
    index = primary_keys.index(primary_key)
    return equal_entity(entities[index], entity)


def remove_entity(entity, entities, primary_field):
    """
    according to the primary key to remove an entity from an entities list
    :param entity: dict
            {"int": 0, "vec": [0.999999, 0.111111]}
    :param entities: list of dict
            [{"int": 0, "vec": [0.999999, 0.111111]}, {"int": 1, "vec": [0.888888, 0.222222]}]
    :param primary_field: collection primary field
    :return: entities of removed entity
    """
    primary_default = ct.default_int64_field_name
    primary_field = primary_default if primary_field is None else primary_field
    primary_key = entity.get(primary_field, None)
    primary_keys = []
    for e in entities:
        primary_keys.append(e[primary_field])
    index = primary_keys.index(primary_key)
    entities.pop(index)
    return entities


def equal_entities_list(exp, actual, primary_field, with_vec=False):
    """
    compare two entities lists in inconsistent order
    :param with_vec: whether entities with vec field
    :param exp: exp entities list, list of dict
    :param actual: actual entities list, list of dict
    :return: True or False
    example:
    exp = [{"int": 0, "vec": [0.999999, 0.111111]}, {"int": 1, "vec": [0.888888, 0.222222]}]
    actual = [{"int": 1, "vec": [0.888888, 0.222222]}, {"int": 0, "vec": [0.999999, 0.111111]}]
    exp = actual
    """
    exp = exp.copy()
    if len(exp) != len(actual):
        return False

    if with_vec:
        for a in actual:
            # if vec field returned in query res
            if entity_in(a, exp, primary_field):
                try:
                    # if vec field returned in query res
                    remove_entity(a, exp, primary_field)
                except Exception as ex:
                    log.error(ex)
    else:
        for a in actual:
            if a in exp:
                try:
                    exp.remove(a)
                except Exception as ex:
                    log.error(ex)
    return True if len(exp) == 0 else False