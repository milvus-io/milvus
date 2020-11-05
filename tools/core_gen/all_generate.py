#!python
# from gen_base_visitor import *
# from gen_node import *
from assemble import *
from meta_gen import *

def gen_file(rootfile, template, output, **kwargs):
    namespace, root_base, struct_name = meta_gen(readfile(rootfile))
    vc = assemble(readfile(template), namespace=namespace, root_base=root_base, struct_name=struct_name, **kwargs)
    file = open(output, 'w')
    file.write(vc)

if __name__ == "__main__":
    query_path = "../../internal/core/src/query/"
    output_path = query_path + "generated/"
    
    
    node_names = ["Expr", "PlanNode"]
    visitor_info = {
        'Expr': [{
            'visitor_name': "ShowExprVisitor",
            "ctor_and_member": ' public:',
            "parameter_name": 'expr',
        }],
        'PlanNode': [{
            'visitor_name': "ShowPlanNodeVisitor",
            "ctor_and_member": ' public:',
            "parameter_name": 'node',
        }]
    }
    
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

