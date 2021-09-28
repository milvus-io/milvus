#!python
from meta_gen import *
import re

def assemble(template, **kwargs):
    pattern = re.compile("@@@@(.*?)\n((.|\n)*?)\n####", re.MULTILINE)
    temp_info = pattern.findall(template)
    # print(temp_info)
    mapping = dict()
    rep_map = dict()

    # drop repetive field from mapping
    for k, v in kwargs.items():
        if isinstance(v, list):
            rep_map[k] = v
        else: 
            mapping[k] = v

    for k, v, _ in temp_info:
        info = k.split("@")
        new_v = replace_all(v, **mapping)
        assert(1 <= len(info) <= 2)  
        if len(info) == 2:
            k = info[0]
            rep = info[1]
            new_v = "\n\n".join([new_v.replace("@@" + rep + "@@", rep_v) for rep_v in rep_map[rep]])
        mapping[k] = new_v
    return mapping["main"]


# import sys
# if __name__ == "__main__":
#     assert(len(sys.argv) == 2)
#     root_file = sys.argv[1]
#     namespace, root_base, struct_name = meta_gen(readfile(root_file))
#     gen_all(readfile("templates/node_full.cpp"), namespace=namespace, root_base=root_base, struct_name=struct_name)


