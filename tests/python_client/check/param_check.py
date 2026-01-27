import sys
import operator
from common import common_type as ct

sys.path.append("..")
from utils.util_log import test_log as log

import numpy as np
from collections.abc import Iterable
import json
from datetime import datetime
from deepdiff import DeepDiff

epsilon = ct.epsilon

def deep_approx_compare(x, y, epsilon=epsilon):
    """
    Recursively compares two objects for approximate equality, handling floating-point precision.
    
    Args:
        x: First object to compare
        y: Second object to compare
        epsilon: Tolerance for floating-point comparisons (default: 1e-6)
    
    Returns:
        bool: True if objects are approximately equal, False otherwise
    
    Handles:
        - Numeric types (int, float, numpy scalars)
        - Sequences (list, tuple, numpy arrays)
        - Dictionaries
        - Other iterables (except strings)
        - Numpy arrays (shape and value comparison)
        - Falls back to strict equality for other types
    """
    # Handle basic numeric types (including numpy scalars)
    if isinstance(x, (int, float, np.integer, np.floating)) and isinstance(y, (int, float, np.integer, np.floating)):
        return abs(float(x) - float(y)) < epsilon

    # Handle lists/tuples/arrays
    if isinstance(x, (list, tuple, np.ndarray)) and isinstance(y, (list, tuple, np.ndarray)):
        if len(x) != len(y):
            return False
        for a, b in zip(x, y):
            if not deep_approx_compare(a, b, epsilon):
                return False
        return True

    # Handle dictionaries
    if isinstance(x, dict) and isinstance(y, dict):
        if set(x.keys()) != set(y.keys()):
            return False
        for key in x:
            if not deep_approx_compare(x[key], y[key], epsilon):
                return False
        return True

    # Handle other iterables (e.g., Protobuf containers)
    if isinstance(x, Iterable) and isinstance(y, Iterable) and not isinstance(x, str):
        try:
            return deep_approx_compare(list(x), list(y), epsilon)
        except:
            pass

    # Handle numpy arrays
    if isinstance(x, np.ndarray) and isinstance(y, np.ndarray):
        if x.shape != y.shape:
            return False
        return np.allclose(x, y, atol=epsilon)

    # Fall back to strict equality for other types
    return x == y


import re
# Pre-compile regex patterns for better performance
_GEO_PATTERN = re.compile(r'(POINT|LINESTRING|POLYGON)\s+\(')
_WHITESPACE_PATTERN = re.compile(r'\s+')

def normalize_geo_string(s):
    """
    Normalize a GEO string by removing extra whitespace.

    Args:
        s: String value that might be a GEO type (POINT, LINESTRING, POLYGON)

    Returns:
        Normalized GEO string or original value if not a GEO string
    """
    if isinstance(s, str) and s.startswith(('POINT', 'LINESTRING', 'POLYGON')):
        s = _GEO_PATTERN.sub(r'\1(', s)
        s = _WHITESPACE_PATTERN.sub(' ', s).strip()
    return s


def normalize_value(value):
    """
    Normalize values for comparison by converting to standard types and formats.
    """
    # Fast path for None and simple immutable types
    if value is None or isinstance(value, (bool, int)):
        return value

    # Convert numpy types to Python native types
    if isinstance(value, (np.integer, np.floating)):
        return float(value) if isinstance(value, np.floating) else int(value)

    # Handle strings (common case for GEO fields)
    if isinstance(value, str):
        return normalize_geo_string(value)

    # Convert list-like protobuf/custom types to standard list
    type_name = type(value).__name__
    if type_name in ('RepeatedScalarContainer', 'HybridExtraList', 'RepeatedCompositeContainer'):
        value = list(value)

    # Handle list of dicts (main use case for search/query results)
    if isinstance(value, (list, tuple)):
        normalized_list = []
        for item in value:
            if isinstance(item, dict):
                # Normalize GEO strings in dict values
                normalized_dict = {}
                for k, v in item.items():
                    if isinstance(v, str):
                        normalized_dict[k] = normalize_geo_string(v)
                    elif isinstance(v, (np.integer, np.floating)):
                        normalized_dict[k] = float(v) if isinstance(v, np.floating) else int(v)
                    elif isinstance(v, np.ndarray):
                        normalized_dict[k] = v.tolist()
                    elif type(v).__name__ in ('RepeatedScalarContainer', 'HybridExtraList', 'RepeatedCompositeContainer'):
                        normalized_dict[k] = list(v)
                    else:
                        normalized_dict[k] = v
                normalized_list.append(normalized_dict)
            else:
                # For non-dict items, just add as-is
                normalized_list.append(item)
        return normalized_list

    # Return as-is for other types
    return value

def compare_lists_with_epsilon_ignore_dict_order(a, b, epsilon=epsilon):
    """
    Compares two lists of dictionaries for equality (order-insensitive) with floating-point tolerance.
    
    Args:
        a (list): First list of dictionaries to compare
        b (list): Second list of dictionaries to compare
        epsilon (float, optional): Tolerance for floating-point comparisons. Defaults to 1e-6.
    
    Returns:
        bool: True if lists contain equivalent dictionaries (order doesn't matter), False otherwise
    
    Note:
        Uses deep_approx_compare() for dictionary comparison with floating-point tolerance.
        Maintains O(n²) complexity due to nested comparisons.
    """
    if len(a) != len(b):
        return False
    a = normalize_value(a)
    b = normalize_value(b)
    # Create a set of available indices for b
    available_indices = set(range(len(b)))

    for item_a in a:
        matched = False
        # Create a list of indices to remove (avoid modifying the set during iteration)
        to_remove = []

        for idx in available_indices:
            if deep_approx_compare(item_a, b[idx], epsilon):
                to_remove.append(idx)
                matched = True
                break

        if not matched:
            return False

        # Remove matched indices
        available_indices -= set(to_remove)

    return True

def compare_lists_with_epsilon_ignore_dict_order_deepdiff(a, b, epsilon=epsilon):
    """
    Compare two lists of dictionaries for equality (order-insensitive) with floating-point tolerance using DeepDiff.
    """
    # Normalize both lists to handle type differences
    a_normalized = normalize_value(a)
    b_normalized = normalize_value(b)

    # Check length first
    if len(a_normalized) != len(b_normalized):
        log.debug(f"[COMPARE_LISTS] Length mismatch: Query result length({len(a_normalized)}) != Expected result length({len(b_normalized)})")
        return False
    
    for i in range(len(a_normalized)):
        diff = DeepDiff(
            a_normalized[i],
            b_normalized[i],
            ignore_order=True,
            math_epsilon=epsilon,
            significant_digits=1,
            ignore_type_in_groups=[(list, tuple)],
            ignore_string_type_changes=True,
        )
        if diff:
            log.debug(f"[COMPARE_LISTS] Found differences at row {i}: {diff}")
            return False
    return True

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
        #  truncate the lists to 100 items in log message
        log.error(f"list_contain_check: List({str(superlist[:20])}...) does not contain list({str(sublist[:20])}...)")
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
    primary_default = ct.default_primary_field_name
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
    primary_default = ct.default_primary_field_name
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


def output_field_value_check(search_res, original, pk_name):
    """
    check if the value of output fields is correct, it only works on auto_id = False
    :param search_res: the search result of specific output fields
    :param original: the data in the collection
    :return: True or False
    """
    pk_name = ct.default_primary_field_name if pk_name is None else pk_name
    nq = len(search_res)
    limit = len(search_res[0])
    check_nqs = min(2, nq)       # the output field values are wrong only at nq>=2  #45338
    for n in range(check_nqs):
        for i in range(limit):
            entity = search_res[n][i].fields
            _id = search_res[n][i].id
            for field in entity.keys():
                if isinstance(entity[field], list):
                    for order in range(0, len(entity[field]), 4):
                        assert abs(original[field][_id][order] - entity[field][order]) < ct.epsilon
                elif isinstance(entity[field], dict) and field != ct.default_json_field_name:
                    # sparse checking, sparse vector must be the last, this is a bit hacky,
                    # but sparse only supports list data type insertion for now
                    assert entity[field].keys() == original[-1][_id].keys()
                else:
                    num = original[original[pk_name] == _id].index.to_list()[0]
                    assert original[field][num] == entity[field], f"the output field values are wrong at nq={n}"

    return True
