#!/usr/bin/env python3

import sys
import argparse
from argparse import Namespace
import os, shutil
import getopt
from ruamel.yaml import YAML, yaml_object
from ruamel.yaml.comments import CommentedSeq, CommentedMap
from ruamel.yaml.tokens import CommentToken

yaml = YAML(typ="rt")
## format yaml file
yaml.indent(mapping=2, sequence=4, offset=2)


############################################
# Comment operation
#
############################################
def _extract_comment(_comment):
    """
    remove '#' at start of comment
    """
    # if _comment is empty, do nothing
    if not _comment:
        return _comment

    # str_ = _comment.lstrip(" ")
    str_ = _comment.strip()
    str_ = str_.lstrip("#")

    return str_


def _add_eol_comment(element, *args, **kwargs):
    """
    add_eol_comment
    args --> (comment, key)
    """
    if element is None or \
            (not isinstance(element, CommentedMap) and
             not isinstance(element, CommentedSeq)) or \
            args[0] is None or \
            len(args[0]) == 0:
        return

    comment = args[0]
    # comment is empty, do nothing
    if not comment:
        return

    key = args[1]
    try:
        element.yaml_add_eol_comment(*args, **kwargs)
    except Exception:
        element.ca.items.pop(key, None)
        element.yaml_add_eol_comment(*args, **kwargs)


def _map_comment(_element, _key):
    origin_comment = ""
    token = _element.ca.items.get(_key, None)
    if token is not None:
        try:
            origin_comment = token[2].value
        except Exception:
            try:
                # comment is below element, add profix "#\n"
                col = _element.lc.col + 2
                space_list = [" " for i in range(col)]
                space_str = "".join(space_list)

                origin_comment = "\n" + "".join([space_str + t.value for t in token[3]])
            except Exception:
                pass

    return origin_comment


def _seq_comment(_element, _index):
    # get target comment
    _comment = ""
    token = _element.ca.items.get(_index, None)
    if token is not None:
        _comment = token[0].value

    return _comment


def _start_comment(_element):
    _comment = ""
    cmt = _element.ca.comment
    try:
        _comment = cmt[1][0].value
    except Exception:
        pass

    return _comment


def _comment_counter(_comment):
    """

    counter comment tips and split into list
    """

    x = lambda l: l.strip().strip("#").strip()

    _counter = []
    if _comment.startswith("\n"):
        _counter.append("")
        _counter.append(x(_comment[1:]))

        return _counter
    elif _comment.startswith("#\n"):
        _counter.append("")
        _counter.append(x(_comment[2:]))
    else:
        index = _comment.find("\n")
        _counter.append(x(_comment[:index]))
        _counter.append(x(_comment[index + 1:]))

    return _counter


def _obtain_comment(_m_comment, _t_comment):
    if not _m_comment or not _t_comment:
        return _m_comment or _t_comment

    _m_counter = _comment_counter(_m_comment)
    _t_counter = _comment_counter(_t_comment)

    if not _m_counter[0] and not _t_counter[1]:
        comment = _t_comment + _m_comment
    elif not _m_counter[1] and not _t_counter[0]:
        comment = _m_comment + _t_comment
    elif _t_counter[0] and _t_counter[1]:
        comment = _t_comment
    elif not _t_counter[0] and not _t_counter[1]:
        comment = _m_comment
    elif not _m_counter[0] and not _m_counter[1]:
        comment = _t_comment
    else:
        if _t_counter[0]:
            comment = _m_comment.replace(_m_counter[0], _t_counter[0], 1)
        else:
            comment = _m_comment.replace(_m_counter[1], _t_counter[1], 1)

    i = comment.find("\n\n")
    while i >= 0:
        comment = comment.replace("\n\n\n", "\n\n", 1)
        i = comment.find("\n\n\n")

    return comment


############################################
# Utils
#
############################################
def _get_update_par(_args):
    _dict = _args.__dict__

    # file path
    _in_file = _dict.get("f", None) or _dict.get("file", None)
    # tips
    _tips = _dict.get('tips', None) or "Input \"-h\" for more information"
    # update
    _u = _dict.get("u", None) or _dict.get("update", None)
    # apppend
    _a = _dict.get('a', None) or _dict.get('append', None)
    # out stream group
    _i = _dict.get("i", None) or _dict.get("inplace", None)
    _o = _dict.get("o", None) or _dict.get("out_file", None)

    return _in_file, _u, _a, _i, _o, _tips


############################################
# Element operation
#
############################################
def update_map_element(element, key, value, comment, _type):
    """
     element:
     key:
     value:
     comment:
     _type:  value type.
    """
    if element is None or not isinstance(element, CommentedMap):
        print("Only key-value update support")
        sys.exit(1)

    origin_comment = _map_comment(element, key)

    sub_element = element.get(key, None)
    if isinstance(sub_element, CommentedMap) or isinstance(sub_element, CommentedSeq):
        print("Only support update a single value")

    element.update({key: value})

    comment = _obtain_comment(origin_comment, comment)
    _add_eol_comment(element, _extract_comment(comment), key)


def update_seq_element(element, value, comment, _type):
    if element is None or not isinstance(element, CommentedSeq):
        print("Param `-a` only use to append yaml list")
        sys.exit(1)
    element.append(str(value))

    comment = _obtain_comment("", comment)
    _add_eol_comment(element, _extract_comment(comment), len(element) - 1)


def run_update(code, keys, value, comment, _app):
    key_list = keys.split(".")

    space_str = ":\n  "
    key_str = "{}".format(key_list[0])
    for key in key_list[1:]:
        key_str = key_str + space_str + key
        space_str = space_str + "  "
    if not _app:
        yaml_str = """{}: {}""".format(key_str, value)
    else:
        yaml_str = "{}{}- {}".format(key_str, space_str, value)

    if comment:
        yaml_str = "{} # {}".format(yaml_str, comment)

    mcode = yaml.load(yaml_str)

    _merge(code, mcode)


def _update(code, _update, _app, _tips):
    if not _update:
        return code

    _update_list = [l.strip() for l in _update.split(",")]
    for l in _update_list:
        try:
            variant, comment = l.split("#")
        except ValueError:
            variant = l
            comment = None

        try:
            keys, value = variant.split("=")
            run_update(code, keys, value, comment, _app)
        except ValueError:
            print("Invalid format. print command \"--help\" get more info.")
            sys.exit(1)

    return code


def _backup(in_file_p):
    backup_p = in_file_p + ".bak"

    if os.path.exists(backup_p):
        os.remove(backup_p)

    if not os.path.exists(in_file_p):
        print("File {} not exists.".format(in_file_p))
        sys.exit(1)

    shutil.copyfile(in_file_p, backup_p)  # 复制文件


def _recovery(in_file_p):
    backup_p = in_file_p + ".bak"

    if not os.path.exists(in_file_p):
        print("File {} not exists.".format(in_file_p))
        sys.exit(1)
    elif not os.path.exists(backup_p):
        print("Backup file not exists")
        sys.exit(0)

    os.remove(in_file_p)

    os.rename(backup_p, in_file_p)


# master merge target
def _merge(master, target):
    if type(master) != type(target):
        print("yaml format not match:\n")
        yaml.dump(master, sys.stdout)
        print("\n&&\n")
        yaml.dump(target, sys.stdout)

        sys.exit(1)

    ## item is a sequence
    if isinstance(target, CommentedSeq):
        for index in range(len(target)):
            # get target comment
            target_comment = _seq_comment(target, index)

            master_index = len(master)

            target_item = target[index]

            if isinstance(target_item, CommentedMap):
                merge_flag = False
                for idx in range(len(master)):
                    if isinstance(master[idx], CommentedMap):
                        if master[idx].keys() == target_item.keys():
                            _merge(master[idx], target_item)
                            # nonlocal merge_flag
                            master_index = idx
                            merge_flag = True
                            break

                if merge_flag is False:
                    master.append(target_item)
            elif target_item not in master:
                master.append(target[index])
            else:
                # merge(master[index], target[index])
                pass

            # # remove enter signal in previous item
            previous_comment = _seq_comment(master, master_index - 1)
            _add_eol_comment(master, _extract_comment(previous_comment), master_index - 1)

            origin_comment = _seq_comment(master, master_index)
            comment = _obtain_comment(origin_comment, target_comment)
            if len(comment) > 0:
                _add_eol_comment(master, _extract_comment(comment) + "\n\n", len(master) - 1)

    ## item is a map
    elif isinstance(target, CommentedMap):
        for item in target:
            if item == "flag":
                print("")
            origin_comment = _map_comment(master, item)
            target_comment = _map_comment(target, item)

            # get origin start comment
            origin_start_comment = _start_comment(master)

            # get target start comment
            target_start_comment = _start_comment(target)

            m = master.get(item, default=None)
            if m is None or \
                    (not (isinstance(m, CommentedMap) or
                          isinstance(m, CommentedSeq))):
                master.update({item: target[item]})

            else:
                _merge(master[item], target[item])

            comment = _obtain_comment(origin_comment, target_comment)
            if len(comment) > 0:
                _add_eol_comment(master, _extract_comment(comment), item)

            start_comment = _obtain_comment(origin_start_comment, target_start_comment)
            if len(start_comment) > 0:
                master.yaml_set_start_comment(_extract_comment(start_comment))


def _save(_code, _file):
    with open(_file, 'w') as wf:
        yaml.dump(_code, wf)


def _load(_file):
    with open(_file, 'r') as rf:
        code = yaml.load(rf)
    return code


############################################
# sub parser process operation
#
############################################
def merge_yaml(_args):
    _dict = _args.__dict__

    _m_file = _dict.get("merge_file", None)
    _in_file, _u, _a, _i, _o, _tips = _get_update_par(_args)

    if not (_in_file and _m_file):
        print(_tips)
        sys.exit(1)

    code = _load(_in_file)
    mcode = _load(_m_file)

    _merge(code, mcode)

    _update(code, _u, _a, _tips)

    if _i:
        _backup(_in_file)
        _save(code, _in_file)
    elif _o:
        _save(code, _o)
    else:
        print(_tips)
        sys.exit(1)


def update_yaml(_args):
    _in_file, _u, _a, _i, _o, _tips = _get_update_par(_args)

    if not _in_file or not _u:
        print(_tips)
        sys.exit(1)

    code = _load(_in_file)

    if _i and _o:
        print(_tips)
        sys.exit(1)

    _update(code, _u, _a, _tips)

    if _i:
        _backup(_in_file)
        _save(code, _in_file)
    elif _o:
        _save(code, _o)


def reset(_args):
    _dict = _args.__dict__
    _f = _dict.get('f', None) or _dict.get('file', None)

    if _f:
        _recovery(_f)
    else:
        _t = _dict.get('tips', None) or "Input \"-h\" for more information"
        print(_t)


############################################
# Cli operation
#
############################################
def _set_merge_parser(_parsers):
    """
    config merge parser
    """

    merge_parser = _parsers.add_parser("merge", help="merge with another yaml file")

    _set_merge_parser_arg(merge_parser)
    _set_update_parser_arg(merge_parser)

    merge_parser.set_defaults(
        function=merge_yaml,
        tips=merge_parser.format_help()
    )


def _set_merge_parser_arg(_parser):
    """
    config parser argument for merging
    """

    _parser.add_argument("-m", "--merge-file", help="indicate merge yaml file")


def _set_update_parser(_parsers):
    """
    config merge parser
    """

    update_parser = _parsers.add_parser("update", help="update with another yaml file")
    _set_update_parser_arg(update_parser)

    update_parser.set_defaults(
        function=update_yaml,
        tips=update_parser.format_help()
    )


def _set_update_parser_arg(_parser):
    """
    config parser argument for updating
    """

    _parser.add_argument("-f", "--file", help="source yaml file")
    _parser.add_argument('-u', '--update', help="update with args, instance as \"a.b.c=d# d comment\"")
    _parser.add_argument('-a', '--append', action="store_true", help="append to a seq")

    group = _parser.add_mutually_exclusive_group()
    group.add_argument("-o", "--out-file", help="indicate output yaml file")
    group.add_argument("-i", "--inplace", action="store_true", help="indicate whether result store in origin file")


def _set_reset_parser(_parsers):
    """
    config merge parser
    """

    reset_parser = _parsers.add_parser("reset", help="reset yaml file")

    # indicate yaml file
    reset_parser.add_argument('-f', '--file', help="indicate input yaml file")

    reset_parser.set_defaults(
        function=reset,
        tips=reset_parser.format_help()
    )


def main():
    parser = argparse.ArgumentParser()
    sub_parsers = parser.add_subparsers()

    # set merge command
    _set_merge_parser(sub_parsers)

    # set update command
    _set_update_parser(sub_parsers)

    # set reset command
    _set_reset_parser(sub_parsers)

    # parse argument and run func
    args = parser.parse_args()
    args.function(args)


if __name__ == '__main__':
    main()
