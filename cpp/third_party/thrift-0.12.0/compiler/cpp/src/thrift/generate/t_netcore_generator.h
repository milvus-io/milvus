#include <cassert>

#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <cctype>

#include <stdlib.h>
#include <sys/stat.h>
#include <sstream>

#include "thrift/platform.h"
#include "thrift/generate/t_oop_generator.h"

using std::map;
using std::ostream;
using std::ostringstream;
using std::string;
using std::stringstream;
using std::vector;

static const string endl = "\n"; // avoid ostream << std::endl flushes

class t_netcore_generator : public t_oop_generator
{

    struct member_mapping_scope
    {
    public:
        member_mapping_scope() : scope_member(0) { }
        void* scope_member;
        map<string, string> mapping_table;
    };

public:
    t_netcore_generator(t_program* program, const map<string, string>& parsed_options, const string& option_string);

    bool is_wcf_enabled() const;
    bool is_nullable_enabled() const;
    bool is_hashcode_enabled() const;
    bool is_serialize_enabled() const;
    bool is_union_enabled() const;
    map<string, int> get_keywords_list() const;

    // overrides
    void init_generator();
    void close_generator();
    void generate_consts(vector<t_const*> consts);
    void generate_consts(ostream& out, vector<t_const*> consts);
    void generate_typedef(t_typedef* ttypedef);
    void generate_enum(t_enum* tenum);
    void generate_enum(ostream& out, t_enum* tenum);
    void generate_struct(t_struct* tstruct);
    void generate_xception(t_struct* txception);
    void generate_service(t_service* tservice);

    void generate_property(ostream& out, t_field* tfield, bool isPublic, bool generateIsset);
    void generate_netcore_property(ostream& out, t_field* tfield, bool isPublic, bool includeIsset = true, string fieldPrefix = "");
    bool print_const_value(ostream& out, string name, t_type* type, t_const_value* value, bool in_static, bool defval = false, bool needtype = false);
    string render_const_value(ostream& out, string name, t_type* type, t_const_value* value);
    void print_const_constructor(ostream& out, vector<t_const*> consts);
    void print_const_def_value(ostream& out, string name, t_type* type, t_const_value* value);
    void generate_netcore_struct(t_struct* tstruct, bool is_exception);
    void generate_netcore_union(t_struct* tunion);
    void generate_netcore_struct_definition(ostream& out, t_struct* tstruct, bool is_xception = false, bool in_class = false, bool is_result = false);
    void generate_netcore_union_definition(ostream& out, t_struct* tunion);
    void generate_netcore_union_class(ostream& out, t_struct* tunion, t_field* tfield);
    void generate_netcore_wcffault(ostream& out, t_struct* tstruct);
    void generate_netcore_struct_reader(ostream& out, t_struct* tstruct);
    void generate_netcore_struct_result_writer(ostream& out, t_struct* tstruct);
    void generate_netcore_struct_writer(ostream& out, t_struct* tstruct);
    void generate_netcore_struct_tostring(ostream& out, t_struct* tstruct);
    void generate_netcore_struct_equals(ostream& out, t_struct* tstruct);
    void generate_netcore_struct_hashcode(ostream& out, t_struct* tstruct);
    void generate_netcore_union_reader(ostream& out, t_struct* tunion);
    void generate_function_helpers(ostream& out, t_function* tfunction);
    void generate_service_interface(ostream& out, t_service* tservice);
    void generate_service_helpers(ostream& out, t_service* tservice);
    void generate_service_client(ostream& out, t_service* tservice);
    void generate_service_server(ostream& out, t_service* tservice);
    void generate_process_function_async(ostream& out, t_service* tservice, t_function* function);
    void generate_deserialize_field(ostream& out, t_field* tfield, string prefix = "", bool is_propertyless = false);
    void generate_deserialize_struct(ostream& out, t_struct* tstruct, string prefix = "");
    void generate_deserialize_container(ostream& out, t_type* ttype, string prefix = "");
    void generate_deserialize_set_element(ostream& out, t_set* tset, string prefix = "");
    void generate_deserialize_map_element(ostream& out, t_map* tmap, string prefix = "");
    void generate_deserialize_list_element(ostream& out, t_list* list, string prefix = "");
    void generate_serialize_field(ostream& out, t_field* tfield, string prefix = "", bool is_element = false, bool is_propertyless = false);
    void generate_serialize_struct(ostream& out, t_struct* tstruct, string prefix = "");
    void generate_serialize_container(ostream& out, t_type* ttype, string prefix = "");
    void generate_serialize_map_element(ostream& out, t_map* tmap, string iter, string map);
    void generate_serialize_set_element(ostream& out, t_set* tmap, string iter);
    void generate_serialize_list_element(ostream& out, t_list* tlist, string iter);
    void generate_netcore_doc(ostream& out, t_field* field);
    void generate_netcore_doc(ostream& out, t_doc* tdoc);
    void generate_netcore_doc(ostream& out, t_function* tdoc);
    void generate_netcore_docstring_comment(ostream& out, string contents);
    void docstring_comment(ostream& out, const string& comment_start, const string& line_prefix, const string& contents, const string& comment_end);
    void start_netcore_namespace(ostream& out);
    void end_netcore_namespace(ostream& out);

    string netcore_type_usings() const;
    string netcore_thrift_usings() const;

    string type_name(t_type* ttype, bool in_countainer = false, bool in_init = false, bool in_param = false, bool is_required = false);
    string base_type_name(t_base_type* tbase, bool in_container = false, bool in_param = false, bool is_required = false);
    string declare_field(t_field* tfield, bool init = false, string prefix = "");
    string function_signature_async(t_function* tfunction, string prefix = "");
    string function_signature(t_function* tfunction, string prefix = "");
    string argument_list(t_struct* tstruct);
    string type_to_enum(t_type* ttype);
    string prop_name(t_field* tfield, bool suppress_mapping = false);
    string get_enum_class_name(t_type* type);

private:
    string namespace_name_;
    string namespace_dir_;

    bool nullable_;
    bool union_;
    bool hashcode_;
    bool serialize_;
    bool wcf_;

    string wcf_namespace_;
    map<string, int> netcore_keywords;
    vector<member_mapping_scope> member_mapping_scopes;

    void init_keywords();
    string normalize_name(string name);
    string make_valid_csharp_identifier(string const& fromName);
    void prepare_member_name_mapping(t_struct* tstruct);
    void prepare_member_name_mapping(void* scope, const vector<t_field*>& members, const string& structname);
    void cleanup_member_name_mapping(void* scope);
    string get_mapped_member_name(string oldname);
};
