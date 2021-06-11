import pytest
import sys
import operator

sys.path.append("..")
from utils.util_log import test_log as log


def ip_check(ip):
    if ip == "localhost":
        return True

    if not isinstance(ip, str):
        log.error("[IP_CHECK] IP(%s) is not a string." % ip)
        return False

    _list = ip.split('.')
    if len(_list) != 4:
        log.error("[IP_CHECK] IP(%s) is wrong, please check manually." % ip)
        return False

    for i in _list:
        if not str(i).isdigit():
            log.error("[IP_CHECK] IP(%s) is wrong, please check manually." % ip)
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

    log.debug("[LIST_DE_DUPLICATION] %s after removing the duplicate elements, the list becomes %s" % (str(_list), str(result)))
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
