def extract_extra_body(visitor_info, query_path):
    pattern = re.compile(r"class(.*){\n((.|\n)*?)\n};", re.MULTILINE)

    for node, visitors in visitor_info.items():
        for visitor in visitors:
            vis_name = visitor['visitor_name']
            vis_file = query_path + "visitors/" + vis_name + ".cpp"
            body = ' public:'

            inc_pattern_str = r'^(#include(.|\n)*)\n#include "query/generated/{}.h"'.format(vis_name)
            inc_pattern = re.compile(inc_pattern_str, re.MULTILINE)

            extra_inc_body = ''
            if os.path.exists(vis_file):
                content = readfile(vis_file)
                infos = pattern.findall(content)
                assert len(infos) <= 1
                if len(infos) == 1:
                    name, body, _ = infos[0]
                
                extra_inc_infos = inc_pattern.findall(content)
                assert(len(extra_inc_infos) <= 1)
                print(extra_inc_infos)
                if len(extra_inc_infos) == 1:
                    extra_inc_body, _ = extra_inc_infos[0]
            
            visitor["ctor_and_member"] = body
            visitor["extra_inc"] = extra_inc_body
