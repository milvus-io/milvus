#!python
import re
import sys

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def readfile(filename):
    file = open(filename)
    content = file.read()
    return content

def replace_all(template, **kwargs):
    for k, v in kwargs.items():
        template = template.replace("@@" + k + "@@", v)
    return template


def meta_gen(content):
    namespace_pattern = re.compile(r"namespace(.*){")
    results = namespace_pattern.findall(content)
    assert(len(results) == 1)
    namespace = results[0].strip()

    struct_pattern = re.compile(r"struct (.*?){((.|\n)*?)^};", re.MULTILINE)
    results = struct_pattern.findall(content)

    body_pattern = re.compile(r"accept\((.*)Visitor ?& ?\) (.*?);")
    # print(results)
    # print(len(results[0]))

    root_base = None
    override_structs = []
    for (title, body, _) in results:
        pack = title.replace(' ', '').split(':')

        if len(pack) == 1:
            pack.append(None)

        body_res = body_pattern.findall(body)
        if len(body_res) != 1:
            continue
            eprint(struct_name)
            eprint(body_res)
            eprint(body)
            assert False
        struct_name, base_name = pack
        if not base_name:
            root_base = struct_name
        visitor_name, state = body_res[0]
        assert(visitor_name == root_base) 
        if state.strip() == 'override':
            override_structs.append(struct_name) 
        # print(body_res)
    return namespace, root_base, override_structs

if __name__ == "__main__":
    assert(len(sys.argv) == 2)
    file = open(sys.argv[1])
    content = file.read()
    namespace, root_base, override_structs = meta_gen(content)
    eprint(namespace)
    eprint(root_base)
    eprint(override_structs)
