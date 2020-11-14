#!python
# from gen_base_visitor import *
# from gen_node import *
from assemble import *
from meta_gen import *
import re
import os

def gen_file(rootfile, template, output, **kwargs):
    namespace, root_base, struct_name = meta_gen(readfile(rootfile))
    vc = assemble(readfile(template), namespace=namespace, root_base=root_base, struct_name=struct_name, **kwargs)
    file = open(output, 'w')
    file.write(vc)


def extract_extra_body(visitor_info, query_path):
    pattern = re.compile("class(.*){\n((.|\n)*?)\n};", re.MULTILINE)

    for node, visitors in visitor_info.items():
        for visitor in visitors:
            vis_name = visitor['visitor_name']
            vis_file = query_path + "visitors/" + vis_name + ".cpp"
            body = ' public:'

            if os.path.exists(vis_file):
                infos = pattern.findall(readfile(vis_file))
                if len(infos) == 1:
                    name, body, _ = infos[0]
            visitor["ctor_and_member"] = body

if __name__ == "__main__":
    query_path = "../../internal/core/src/query/"
    output_path = query_path + "generated/"
    
    
    node_names = ["Expr", "PlanNode"]
    visitor_info = {
        'Expr': [{
            'visitor_name': "ShowExprVisitor",
            "parameter_name": 'expr',
        }],
        'PlanNode': [
            {
                'visitor_name': "ShowPlanNodeVisitor",
                "parameter_name": 'node',
            },
            {
                'visitor_name': "ExecPlanNodeVisitor",
                "parameter_name": 'node',
            },

        ]
    }
    extract_extra_body(visitor_info, query_path)
    
    for name in node_names:
        rootfile = query_path + name + ".h"

        template = 'templates/visitor_base.h'
        output = output_path + name + 'Visitor.h'
        gen_file(rootfile, template, output)

        template = 'templates/node_def.cpp'
        output = output_path + name + '.cpp'
        gen_file(rootfile, template, output)

        for info in visitor_info[name]:
            vis_name = info['visitor_name']
            template = 'templates/visitor_derived.h'
            output = output_path + vis_name + '.h'
            gen_file(rootfile, template, output, **info)

            vis_name = info['visitor_name']
            template = 'templates/visitor_derived.cpp'
            output = output_path + vis_name + '.cpp'
            gen_file(rootfile, template, output, **info)
    print("Done")
