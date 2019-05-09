/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

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
using std::ofstream;
using std::ostringstream;
using std::string;
using std::stringstream;
using std::vector;

//TODO: check for indentation 
//TODO: Do we need seqId_ in generation?

static const string endl = "\n"; // avoid ostream << std::endl flushes

struct member_mapping_scope
{
    void* scope_member;
    map<string, string> mapping_table;
};

class t_netcore_generator : public t_oop_generator
{
public:
    t_netcore_generator(t_program* program, const map<string, string>& parsed_options, const string& option_string)
        : t_oop_generator(program)
    {
        (void)option_string;

        nullable_ = false;
        hashcode_ = false;
        union_ = false;
        serialize_ = false;
        wcf_ = false;
        wcf_namespace_.clear();

        map<string, string>::const_iterator iter;

        for (iter = parsed_options.begin(); iter != parsed_options.end(); ++iter)
        {
            if (iter->first.compare("nullable") == 0)
            {
                nullable_ = true;
            }
            else if (iter->first.compare("hashcode") == 0)
            {
                hashcode_ = true;
            }
            else if (iter->first.compare("union") == 0)
            {
                union_ = true;
            }
            else if (iter->first.compare("serial") == 0)
            {
                serialize_ = true;
                wcf_namespace_ = iter->second; // since there can be only one namespace
            }
            else if (iter->first.compare("wcf") == 0)
            {
                wcf_ = true;
                wcf_namespace_ = iter->second;
            }
            else
            {
                throw "unknown option netcore:" + iter->first;
            }
        }

        out_dir_base_ = "gen-netcore";
    }

    // overrides
    void init_generator();
    void close_generator();
    void generate_consts(vector<t_const*> consts);
    void generate_typedef(t_typedef* ttypedef);
    void generate_enum(t_enum* tenum);
    void generate_struct(t_struct* tstruct);
    void generate_xception(t_struct* txception);
    void generate_service(t_service* tservice);

    void generate_property(ofstream& out, t_field* tfield, bool isPublic, bool generateIsset);
    void generate_netcore_property(ofstream& out, t_field* tfield, bool isPublic, bool includeIsset = true, string fieldPrefix = "");
    bool print_const_value(ofstream& out, string name, t_type* type, t_const_value* value, bool in_static, bool defval = false, bool needtype = false);
    string render_const_value(ofstream& out, string name, t_type* type, t_const_value* value);
    void print_const_constructor(ofstream& out, vector<t_const*> consts);
    void print_const_def_value(ofstream& out, string name, t_type* type, t_const_value* value);
    void generate_netcore_struct(t_struct* tstruct, bool is_exception);
    void generate_netcore_union(t_struct* tunion);
    void generate_netcore_struct_definition(ofstream& out, t_struct* tstruct, bool is_xception = false, bool in_class = false, bool is_result = false);
    void generate_netcore_union_definition(ofstream& out, t_struct* tunion);
    void generate_netcore_union_class(ofstream& out, t_struct* tunion, t_field* tfield);
    void generate_netcore_wcffault(ofstream& out, t_struct* tstruct);
    void generate_netcore_struct_reader(ofstream& out, t_struct* tstruct);
    void generate_netcore_struct_result_writer(ofstream& out, t_struct* tstruct);
    void generate_netcore_struct_writer(ofstream& out, t_struct* tstruct);
    void generate_netcore_struct_tostring(ofstream& out, t_struct* tstruct);
    void generate_netcore_struct_equals(ofstream& out, t_struct* tstruct);
    void generate_netcore_struct_hashcode(ofstream& out, t_struct* tstruct);
    void generate_netcore_union_reader(ofstream& out, t_struct* tunion);
    void generate_function_helpers(ofstream& out, t_function* tfunction);
    void generate_service_interface(ofstream& out, t_service* tservice);
    void generate_service_helpers(ofstream& out, t_service* tservice);
    void generate_service_client(ofstream& out, t_service* tservice);
    void generate_service_server(ofstream& out, t_service* tservice);
    void generate_process_function_async(ofstream& out, t_service* tservice, t_function* function);
    void generate_deserialize_field(ofstream& out, t_field* tfield, string prefix = "", bool is_propertyless = false);
    void generate_deserialize_struct(ofstream& out, t_struct* tstruct, string prefix = "");
    void generate_deserialize_container(ofstream& out, t_type* ttype, string prefix = "");
    void generate_deserialize_set_element(ofstream& out, t_set* tset, string prefix = "");
    void generate_deserialize_map_element(ofstream& out, t_map* tmap, string prefix = "");
    void generate_deserialize_list_element(ofstream& out, t_list* list, string prefix = "");
    void generate_serialize_field(ofstream& out, t_field* tfield, string prefix = "", bool is_element = false, bool is_propertyless = false);
    void generate_serialize_struct(ofstream& out, t_struct* tstruct, string prefix = "");
    void generate_serialize_container(ofstream& out, t_type* ttype, string prefix = "");
    void generate_serialize_map_element(ofstream& out, t_map* tmap, string iter, string map);
    void generate_serialize_set_element(ofstream& out, t_set* tmap, string iter);
    void generate_serialize_list_element(ofstream& out, t_list* tlist, string iter);
    void generate_netcore_doc(ofstream& out, t_field* field);
    void generate_netcore_doc(ofstream& out, t_doc* tdoc);
    void generate_netcore_doc(ofstream& out, t_function* tdoc);
    void generate_netcore_docstring_comment(ofstream& out, string contents);
    void docstring_comment(ofstream& out, const string& comment_start, const string& line_prefix, const string& contents, const string& comment_end);
    void start_netcore_namespace(ofstream& out);
    void end_netcore_namespace(ofstream& out);

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

    static string correct_function_name_for_async(string const& function_name)
    {
        string const async_end = "Async";
        size_t i = function_name.find(async_end);
        if (i != string::npos)
        {
            return function_name + async_end;
        }

        return function_name;
    }

    /**
     * \brief Search and replace "_args" substring in struct name if exist (for C# class naming)
     * \param struct_name
     * \return Modified struct name ("Struct_args" -> "StructArgs") or original name
     */
    static string check_and_correct_struct_name(const string& struct_name)
    {
        string args_end = "_args";
        size_t i = struct_name.find(args_end);
        if (i != string::npos)
        {
            string new_struct_name = struct_name;
            new_struct_name.replace(i, args_end.length(), "Args");
            return new_struct_name;
        }

        string result_end = "_result";
        size_t j = struct_name.find(result_end);
        if (j != string::npos)
        {
            string new_struct_name = struct_name;
            new_struct_name.replace(j, result_end.length(), "Result");
            return new_struct_name;
        }

        return struct_name;
    }

    static bool field_has_default(t_field* tfield) { return tfield->get_value() != NULL; }

    static bool field_is_required(t_field* tfield) { return tfield->get_req() == t_field::T_REQUIRED; }

    static bool type_can_be_null(t_type* ttype)
    {
        while (ttype->is_typedef())
        {
            ttype = static_cast<t_typedef*>(ttype)->get_type();
        }

        return ttype->is_container() || ttype->is_struct() || ttype->is_xception() || ttype->is_string();
    }


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

void t_netcore_generator::init_generator()
{
    MKDIR(get_out_dir().c_str());

    // for usage of csharp namespaces in thrift files (from files for csharp)
    namespace_name_ = program_->get_namespace("netcore");
    if (namespace_name_.empty())
    {
        namespace_name_ = program_->get_namespace("netcore");
    }

    string dir = namespace_name_;
    string subdir = get_out_dir().c_str();
    string::size_type loc;

    while ((loc = dir.find(".")) != string::npos)
    {
        subdir = subdir + "/" + dir.substr(0, loc);
        MKDIR(subdir.c_str());
        dir = dir.substr(loc + 1);
    }
    if (dir.size() > 0)
    {
        subdir = subdir + "/" + dir;
        MKDIR(subdir.c_str());
    }

    namespace_dir_ = subdir;
    init_keywords();

    while (!member_mapping_scopes.empty())
    {
        cleanup_member_name_mapping(member_mapping_scopes.back().scope_member);
    }

    pverbose(".NET Core options:\n");
    pverbose("- nullable ... %s\n", (nullable_ ? "ON" : "off"));
    pverbose("- union ...... %s\n", (union_ ? "ON" : "off"));
    pverbose("- hashcode ... %s\n", (hashcode_ ? "ON" : "off"));
    pverbose("- serialize .. %s\n", (serialize_ ? "ON" : "off"));
    pverbose("- wcf ........ %s\n", (wcf_ ? "ON" : "off"));
}

string t_netcore_generator::normalize_name(string name)
{
    string tmp(name);
    transform(tmp.begin(), tmp.end(), tmp.begin(), static_cast<int(*)(int)>(tolower));

    // un-conflict keywords by prefixing with "@"
    if (netcore_keywords.find(tmp) != netcore_keywords.end())
    {
        return "@" + name;
    }

    // no changes necessary
    return name;
}

void t_netcore_generator::init_keywords()
{
    netcore_keywords.clear();

    // C# keywords
    netcore_keywords["abstract"] = 1;
    netcore_keywords["as"] = 1;
    netcore_keywords["base"] = 1;
    netcore_keywords["bool"] = 1;
    netcore_keywords["break"] = 1;
    netcore_keywords["byte"] = 1;
    netcore_keywords["case"] = 1;
    netcore_keywords["catch"] = 1;
    netcore_keywords["char"] = 1;
    netcore_keywords["checked"] = 1;
    netcore_keywords["class"] = 1;
    netcore_keywords["const"] = 1;
    netcore_keywords["continue"] = 1;
    netcore_keywords["decimal"] = 1;
    netcore_keywords["default"] = 1;
    netcore_keywords["delegate"] = 1;
    netcore_keywords["do"] = 1;
    netcore_keywords["double"] = 1;
    netcore_keywords["else"] = 1;
    netcore_keywords["enum"] = 1;
    netcore_keywords["event"] = 1;
    netcore_keywords["explicit"] = 1;
    netcore_keywords["extern"] = 1;
    netcore_keywords["false"] = 1;
    netcore_keywords["finally"] = 1;
    netcore_keywords["fixed"] = 1;
    netcore_keywords["float"] = 1;
    netcore_keywords["for"] = 1;
    netcore_keywords["foreach"] = 1;
    netcore_keywords["goto"] = 1;
    netcore_keywords["if"] = 1;
    netcore_keywords["implicit"] = 1;
    netcore_keywords["in"] = 1;
    netcore_keywords["int"] = 1;
    netcore_keywords["interface"] = 1;
    netcore_keywords["internal"] = 1;
    netcore_keywords["is"] = 1;
    netcore_keywords["lock"] = 1;
    netcore_keywords["long"] = 1;
    netcore_keywords["namespace"] = 1;
    netcore_keywords["new"] = 1;
    netcore_keywords["null"] = 1;
    netcore_keywords["object"] = 1;
    netcore_keywords["operator"] = 1;
    netcore_keywords["out"] = 1;
    netcore_keywords["override"] = 1;
    netcore_keywords["params"] = 1;
    netcore_keywords["private"] = 1;
    netcore_keywords["protected"] = 1;
    netcore_keywords["public"] = 1;
    netcore_keywords["readonly"] = 1;
    netcore_keywords["ref"] = 1;
    netcore_keywords["return"] = 1;
    netcore_keywords["sbyte"] = 1;
    netcore_keywords["sealed"] = 1;
    netcore_keywords["short"] = 1;
    netcore_keywords["sizeof"] = 1;
    netcore_keywords["stackalloc"] = 1;
    netcore_keywords["static"] = 1;
    netcore_keywords["string"] = 1;
    netcore_keywords["struct"] = 1;
    netcore_keywords["switch"] = 1;
    netcore_keywords["this"] = 1;
    netcore_keywords["throw"] = 1;
    netcore_keywords["true"] = 1;
    netcore_keywords["try"] = 1;
    netcore_keywords["typeof"] = 1;
    netcore_keywords["uint"] = 1;
    netcore_keywords["ulong"] = 1;
    netcore_keywords["unchecked"] = 1;
    netcore_keywords["unsafe"] = 1;
    netcore_keywords["ushort"] = 1;
    netcore_keywords["using"] = 1;
    netcore_keywords["virtual"] = 1;
    netcore_keywords["void"] = 1;
    netcore_keywords["volatile"] = 1;
    netcore_keywords["while"] = 1;

    // C# contextual keywords
    netcore_keywords["add"] = 1;
    netcore_keywords["alias"] = 1;
    netcore_keywords["ascending"] = 1;
    netcore_keywords["async"] = 1;
    netcore_keywords["await"] = 1;
    netcore_keywords["descending"] = 1;
    netcore_keywords["dynamic"] = 1;
    netcore_keywords["from"] = 1;
    netcore_keywords["get"] = 1;
    netcore_keywords["global"] = 1;
    netcore_keywords["group"] = 1;
    netcore_keywords["into"] = 1;
    netcore_keywords["join"] = 1;
    netcore_keywords["let"] = 1;
    netcore_keywords["orderby"] = 1;
    netcore_keywords["partial"] = 1;
    netcore_keywords["remove"] = 1;
    netcore_keywords["select"] = 1;
    netcore_keywords["set"] = 1;
    netcore_keywords["value"] = 1;
    netcore_keywords["var"] = 1;
    netcore_keywords["where"] = 1;
    netcore_keywords["yield"] = 1;
}

void t_netcore_generator::start_netcore_namespace(ofstream& out)
{
    if (!namespace_name_.empty())
    {
        out << "namespace " << namespace_name_ << endl;
        scope_up(out);
    }
}

void t_netcore_generator::end_netcore_namespace(ofstream& out)
{
    if (!namespace_name_.empty())
    {
        scope_down(out);
    }
}

string t_netcore_generator::netcore_type_usings() const
{
    string namespaces =
        "using System;\n"
        "using System.Collections;\n"
        "using System.Collections.Generic;\n"
        "using System.Text;\n"
        "using System.IO;\n"
        "using System.Threading;\n"
        "using System.Threading.Tasks;\n"
        "using Thrift;\n"
        "using Thrift.Collections;\n";

    if (wcf_)
    {
        namespaces += "using System.ServiceModel;\n";
        namespaces += "using System.Runtime.Serialization;\n";
    }

    return namespaces + endl;
}

string t_netcore_generator::netcore_thrift_usings() const
{
    string namespaces =
        "using Thrift.Protocols;\n"
        "using Thrift.Protocols.Entities;\n"
        "using Thrift.Protocols.Utilities;\n"
        "using Thrift.Transports;\n"
        "using Thrift.Transports.Client;\n"
        "using Thrift.Transports.Server;\n";

    return namespaces + endl;
}

void t_netcore_generator::close_generator()
{
}

void t_netcore_generator::generate_typedef(t_typedef* ttypedef)
{
    (void)ttypedef;
}

void t_netcore_generator::generate_enum(t_enum* tenum)
{
    int ic = indent_count();

    string f_enum_name = namespace_dir_ + "/" + tenum->get_name() + ".cs";

    ofstream f_enum;
    f_enum.open(f_enum_name.c_str());
    f_enum << autogen_comment() << endl;

    start_netcore_namespace(f_enum);
    generate_netcore_doc(f_enum, tenum);

    f_enum << indent() << "public enum " << tenum->get_name() << endl;
    scope_up(f_enum);

    vector<t_enum_value*> constants = tenum->get_constants();
    vector<t_enum_value*>::iterator c_iter;

    for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter)
    {
        generate_netcore_doc(f_enum, *c_iter);
        int value = (*c_iter)->get_value();
        f_enum << indent() << (*c_iter)->get_name() << " = " << value << "," << endl;
    }

    scope_down(f_enum);
    end_netcore_namespace(f_enum);
    f_enum.close();

    indent_validate(ic, "generate_enum");
}

void t_netcore_generator::generate_consts(vector<t_const*> consts)
{
    if (consts.empty())
    {
        return;
    }

    string f_consts_name = namespace_dir_ + '/' + program_name_ + ".Constants.cs";
    ofstream f_consts;
    f_consts.open(f_consts_name.c_str());

    f_consts << autogen_comment() << netcore_type_usings() << endl;

    start_netcore_namespace(f_consts);

    f_consts << indent() << "public static class " << make_valid_csharp_identifier(program_name_) << "Constants" << endl;

    scope_up(f_consts);

    vector<t_const*>::iterator c_iter;
    bool need_static_constructor = false;
    for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter)
    {
        generate_netcore_doc(f_consts, *c_iter);
        if (print_const_value(f_consts, (*c_iter)->get_name(), (*c_iter)->get_type(), (*c_iter)->get_value(), false))
        {
            need_static_constructor = true;
        }
    }

    if (need_static_constructor)
    {
        print_const_constructor(f_consts, consts);
    }

    scope_down(f_consts);
    end_netcore_namespace(f_consts);
    f_consts.close();
}

void t_netcore_generator::print_const_def_value(ofstream& out, string name, t_type* type, t_const_value* value)
{
    if (type->is_struct() || type->is_xception())
    {
        const vector<t_field*>& fields = static_cast<t_struct*>(type)->get_members();
        const map<t_const_value*, t_const_value*>& val = value->get_map();
        vector<t_field*>::const_iterator f_iter;
        map<t_const_value*, t_const_value*>::const_iterator v_iter;
        prepare_member_name_mapping(static_cast<t_struct*>(type));

        for (v_iter = val.begin(); v_iter != val.end(); ++v_iter)
        {
            t_field* field = NULL;
            
            for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
            {
                if ((*f_iter)->get_name() == v_iter->first->get_string())
                {
                    field = *f_iter;
                }
            }

            if (field == NULL)
            {
                throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
            }

            t_type* field_type = field->get_type();
            
            string val = render_const_value(out, name, field_type, v_iter->second);
            out << indent() << name << "." << prop_name(field) << " = " << val << ";" << endl;
        }

        cleanup_member_name_mapping(static_cast<t_struct*>(type));
    }
    else if (type->is_map())
    {
        t_type* ktype = static_cast<t_map*>(type)->get_key_type();
        t_type* vtype = static_cast<t_map*>(type)->get_val_type();
        const map<t_const_value*, t_const_value*>& val = value->get_map();
        map<t_const_value*, t_const_value*>::const_iterator v_iter;
        for (v_iter = val.begin(); v_iter != val.end(); ++v_iter)
        {
            string key = render_const_value(out, name, ktype, v_iter->first);
            string val = render_const_value(out, name, vtype, v_iter->second);
            out << indent() << name << "[" << key << "]" << " = " << val << ";" << endl;
        }
    }
    else if (type->is_list() || type->is_set())
    {
        t_type* etype;
        if (type->is_list())
        {
            etype = static_cast<t_list*>(type)->get_elem_type();
        }
        else
        {
            etype = static_cast<t_set*>(type)->get_elem_type();
        }

        const vector<t_const_value*>& val = value->get_list();
        vector<t_const_value*>::const_iterator v_iter;
        for (v_iter = val.begin(); v_iter != val.end(); ++v_iter)
        {
            string val = render_const_value(out, name, etype, *v_iter);
            out << indent() << name << ".Add(" << val << ");" << endl;
        }
    }
}

void t_netcore_generator::print_const_constructor(ofstream& out, vector<t_const*> consts)
{
    out << indent() << "static " << make_valid_csharp_identifier(program_name_).c_str() << "Constants()" << endl;
    scope_up(out);

    vector<t_const*>::iterator c_iter;
    for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter)
    {
        string name = (*c_iter)->get_name();
        t_type* type = (*c_iter)->get_type();
        t_const_value* value = (*c_iter)->get_value();

        print_const_def_value(out, name, type, value);
    }
    scope_down(out);
}

bool t_netcore_generator::print_const_value(ofstream& out, string name, t_type* type, t_const_value* value, bool in_static, bool defval, bool needtype)
{
    out << indent();
    bool need_static_construction = !in_static;
    while (type->is_typedef())
    {
        type = static_cast<t_typedef*>(type)->get_type();
    }

    if (!defval || needtype)
    {
        out << (in_static ? "" : type->is_base_type() ? "public const " : "public static ") << type_name(type) << " ";
    }

    if (type->is_base_type())
    {
        string v2 = render_const_value(out, name, type, value);
        out << name << " = " << v2 << ";" << endl;
        need_static_construction = false;
    }
    else if (type->is_enum())
    {
        out << name << " = " << type_name(type, false, true) << "." << value->get_identifier_name() << ";" << endl;
        need_static_construction = false;
    }
    else if (type->is_struct() || type->is_xception())
    {
        out << name << " = new " << type_name(type) << "();" << endl;
    }
    else if (type->is_map())
    {
        out << name << " = new " << type_name(type, true, true) << "();" << endl;
    }
    else if (type->is_list() || type->is_set())
    {
        out << name << " = new " << type_name(type) << "();" << endl;
    }

    if (defval && !type->is_base_type() && !type->is_enum())
    {
        print_const_def_value(out, name, type, value);
    }

    return need_static_construction;
}

string t_netcore_generator::render_const_value(ofstream& out, string name, t_type* type, t_const_value* value)
{
    (void)name;
    ostringstream render;

    if (type->is_base_type())
    {
        t_base_type::t_base tbase = static_cast<t_base_type*>(type)->get_base();
        switch (tbase)
        {
        case t_base_type::TYPE_STRING:
            render << '"' << get_escaped_string(value) << '"';
            break;
        case t_base_type::TYPE_BOOL:
            render << ((value->get_integer() > 0) ? "true" : "false");
            break;
        case t_base_type::TYPE_I8:
        case t_base_type::TYPE_I16:
        case t_base_type::TYPE_I32:
        case t_base_type::TYPE_I64:
            render << value->get_integer();
            break;
        case t_base_type::TYPE_DOUBLE:
            if (value->get_type() == t_const_value::CV_INTEGER)
            {
                render << value->get_integer();
            }
            else
            {
                render << value->get_double();
            }
            break;
        default:
            throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
        }
    }
    else if (type->is_enum())
    {
        render << type->get_name() << "." << value->get_identifier_name();
    }
    else
    {
        string t = tmp("tmp");
        print_const_value(out, t, type, value, true, true, true);
        render << t;
    }

    return render.str();
}

void t_netcore_generator::generate_struct(t_struct* tstruct)
{
    if (union_ && tstruct->is_union())
    {
        generate_netcore_union(tstruct);
    }
    else
    {
        generate_netcore_struct(tstruct, false);
    }
}

void t_netcore_generator::generate_xception(t_struct* txception)
{
    generate_netcore_struct(txception, true);
}

void t_netcore_generator::generate_netcore_struct(t_struct* tstruct, bool is_exception)
{
    int ic = indent_count();

    string f_struct_name = namespace_dir_ + "/" + (tstruct->get_name()) + ".cs";
    ofstream f_struct;

    f_struct.open(f_struct_name.c_str());

    f_struct << autogen_comment() << netcore_type_usings() << netcore_thrift_usings() << endl;

    generate_netcore_struct_definition(f_struct, tstruct, is_exception);

    f_struct.close();

    indent_validate(ic, "generate_netcore_struct");
}

void t_netcore_generator::generate_netcore_struct_definition(ofstream& out, t_struct* tstruct, bool is_exception, bool in_class, bool is_result)
{
    if (!in_class)
    {
        start_netcore_namespace(out);
    }

    out << endl;

    generate_netcore_doc(out, tstruct);
    prepare_member_name_mapping(tstruct);

    if ((serialize_ || wcf_) && !is_exception)
    {
        out << indent() << "[DataContract(Namespace=\"" << wcf_namespace_ << "\")]" << endl;
    }

    bool is_final = tstruct->annotations_.find("final") != tstruct->annotations_.end();

    string sharp_struct_name = check_and_correct_struct_name(normalize_name(tstruct->get_name()));

    out << indent() << "public " << (is_final ? "sealed " : "") << "partial class " << sharp_struct_name << " : ";

    if (is_exception) 
    {
        out << "TException, ";
    }

    out << "TBase" << endl
        << indent() << "{" << endl;
    indent_up();

    const vector<t_field*>& members = tstruct->get_members();
    vector<t_field*>::const_iterator m_iter;

    // make private members with public Properties
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
    {
        // if the field is requied, then we use auto-properties
        if (!field_is_required((*m_iter)) && (!nullable_ || field_has_default((*m_iter))))
        {
            out << indent() << "private " << declare_field(*m_iter, false, "_") << endl;
        }
    }
    out << endl;

    bool has_non_required_fields = false;
    bool has_non_required_default_value_fields = false;
    bool has_required_fields = false;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
    {
        generate_netcore_doc(out, *m_iter);
        generate_property(out, *m_iter, true, true);
        bool is_required = field_is_required((*m_iter));
        bool has_default = field_has_default((*m_iter));
        if (is_required)
        {
            has_required_fields = true;
        }
        else
        {
            if (has_default)
            {
                has_non_required_default_value_fields = true;
            }
            has_non_required_fields = true;
        }
    }

    bool generate_isset = (nullable_ && has_non_required_default_value_fields) || (!nullable_ && has_non_required_fields);
    if (generate_isset)
    {
        out << endl;
        if (serialize_ || wcf_)
        {
            out << indent() << "[DataMember(Order = 1)]" << endl;
        }
        out << indent() << "public Isset __isset;" << endl;
        if (serialize_ || wcf_)
        {
            out << indent() << "[DataContract]" << endl;
        }

        out << indent() << "public struct Isset" << endl
            << indent() << "{" << endl;
        indent_up();
        
        for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
        {
            bool is_required = field_is_required((*m_iter));
            bool has_default = field_has_default((*m_iter));
            // if it is required, don't need Isset for that variable
            // if it is not required, if it has a default value, we need to generate Isset
            // if we are not nullable, then we generate Isset
            if (!is_required && (!nullable_ || has_default))
            {
                if (serialize_ || wcf_)
                {
                    out << indent() << "[DataMember]" << endl;
                }
                out << indent() << "public bool " << normalize_name((*m_iter)->get_name()) << ";" << endl;
            }
        }

        indent_down();
        out << indent() << "}" << endl << endl;

        if (generate_isset && (serialize_ || wcf_))
        {
            out << indent() << "#region XmlSerializer support" << endl << endl;

            for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
            {
                bool is_required = field_is_required(*m_iter);
                bool has_default = field_has_default(*m_iter);
                // if it is required, don't need Isset for that variable
                // if it is not required, if it has a default value, we need to generate Isset
                // if we are not nullable, then we generate Isset
                if (!is_required && (!nullable_ || has_default))
                {
                    out << indent() << "public bool ShouldSerialize" << prop_name(*m_iter) << "()" << endl
                        << indent() << "{" << endl;
                    indent_up();
                    out << indent() << "return __isset." << normalize_name((*m_iter)->get_name()) << ";" << endl;
                    indent_down();
                    out << indent() << "}" << endl << endl;
                }
            }

            out << indent() << "#endregion XmlSerializer support" << endl << endl;
        }
    }

    // We always want a default, no argument constructor for Reading
    out << indent() << "public " << sharp_struct_name << "()" << endl
        << indent() << "{" << endl;
    indent_up();

    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
    {
        t_type* t = (*m_iter)->get_type();
        while (t->is_typedef())
        {
            t = static_cast<t_typedef*>(t)->get_type();
        }
        if ((*m_iter)->get_value() != NULL)
        {
            if (field_is_required((*m_iter)))
            {
                print_const_value(out, "this." + prop_name(*m_iter), t, (*m_iter)->get_value(), true, true);
            }
            else
            {
                print_const_value(out, "this._" + (*m_iter)->get_name(), t, (*m_iter)->get_value(), true, true);
                // Optionals with defaults are marked set
                out << indent() << "this.__isset." << normalize_name((*m_iter)->get_name()) << " = true;" << endl;
            }
        }
    }
    indent_down();
    out << indent() << "}" << endl << endl;

    if (has_required_fields)
    {
        out << indent() << "public " << sharp_struct_name << "(";
        bool first = true;
        for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
        {
            if (field_is_required(*m_iter))
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    out << ", ";
                }
                out << type_name((*m_iter)->get_type()) << " " << (*m_iter)->get_name();
            }
        }
        out << ") : this()" << endl
            << indent() << "{" << endl;
        indent_up();

        for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
        {
            if (field_is_required(*m_iter))
            {
                out << indent() << "this." << prop_name(*m_iter) << " = " << (*m_iter)->get_name() << ";" << endl;
            }
        }

        indent_down();
        out << indent() << "}" << endl << endl;
    }

    generate_netcore_struct_reader(out, tstruct);
    if (is_result)
    {
        generate_netcore_struct_result_writer(out, tstruct);
    }
    else
    {
        generate_netcore_struct_writer(out, tstruct);
    }
    if (hashcode_)
    {
        generate_netcore_struct_equals(out, tstruct);
        generate_netcore_struct_hashcode(out, tstruct);
    }
    generate_netcore_struct_tostring(out, tstruct);
    
    indent_down();
    out << indent() << "}" << endl << endl;

    // generate a corresponding WCF fault to wrap the exception
    if ((serialize_ || wcf_) && is_exception)
    {
        generate_netcore_wcffault(out, tstruct);
    }

    cleanup_member_name_mapping(tstruct);
    if (!in_class)
    {
        end_netcore_namespace(out);
    }
}

void t_netcore_generator::generate_netcore_wcffault(ofstream& out, t_struct* tstruct)
{
    out << endl;
    out << indent() << "[DataContract]" << endl;

    bool is_final = tstruct->annotations_.find("final") != tstruct->annotations_.end();

    out << indent() << "public " << (is_final ? "sealed " : "") << "partial class " << tstruct->get_name() << "Fault" << endl
        << indent() << "{" << endl;
    indent_up();

    const vector<t_field*>& members = tstruct->get_members();
    vector<t_field*>::const_iterator m_iter;

    // make private members with public Properties
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
    {
        out << indent() << "private " << declare_field(*m_iter, false, "_") << endl;
    }
    out << endl;

    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter)
    {
        generate_property(out, *m_iter, true, false);
    }

    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_struct_reader(ofstream& out, t_struct* tstruct)
{
    out << indent() << "public async Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "iprot.IncrementRecursionDepth();" << endl
        << indent() << "try" << endl
        << indent() << "{" << endl;
    indent_up();
    
    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;

    // Required variables aren't in __isset, so we need tmp vars to check them
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        if (field_is_required(*f_iter))
        {
            out << indent() << "bool isset_" << (*f_iter)->get_name() << " = false;" << endl;
        }
    }

    out << indent() << "TField field;" << endl 
        << indent() << "await iprot.ReadStructBeginAsync(cancellationToken);" << endl
        << indent() << "while (true)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "field = await iprot.ReadFieldBeginAsync(cancellationToken);" << endl
        << indent() << "if (field.Type == TType.Stop)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "break;" << endl;
    indent_down();
    out << indent() << "}" << endl << endl
        << indent() << "switch (field.ID)" << endl
        << indent() << "{" << endl;
    indent_up();
    
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        bool is_required = field_is_required(*f_iter);
        out << indent() << "case " << (*f_iter)->get_key() << ":" << endl;
        indent_up();
        out << indent() << "if (field.Type == " << type_to_enum((*f_iter)->get_type()) << ")" << endl
            << indent() << "{" << endl;
        indent_up();

        generate_deserialize_field(out, *f_iter);
        if (is_required)
        {
            out << indent() << "isset_" << (*f_iter)->get_name() << " = true;" << endl;
        }

        indent_down();
        out << indent() << "}" << endl
            << indent() << "else" << endl
            << indent() << "{" << endl;
        indent_up();
        out << indent() << "await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);" << endl;
        indent_down();
        out << indent() << "}" << endl
            << indent() << "break;" << endl;
        indent_down();
    }

    out << indent() << "default: " << endl;
    indent_up();
    out << indent() << "await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);" << endl
        << indent() << "break;" << endl;
    indent_down();
    indent_down();
    out << indent() << "}" << endl
        << endl
        << indent() << "await iprot.ReadFieldEndAsync(cancellationToken);" << endl;
    indent_down();
    out << indent() << "}" << endl
        << endl
        << indent() << "await iprot.ReadStructEndAsync(cancellationToken);" << endl;

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        if (field_is_required((*f_iter)))
        {
            out << indent() << "if (!isset_" << (*f_iter)->get_name() << ")" << endl
                << indent() << "{" << endl;
            indent_up();
            out << indent() << "throw new TProtocolException(TProtocolException.INVALID_DATA);" << endl;
            indent_down();
            out << indent() << "}" << endl;
        }
    }
    
    indent_down();
    out << indent() << "}" << endl;
    out << indent() << "finally" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "iprot.DecrementRecursionDepth();" << endl;
    indent_down();
    out << indent() << "}" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_struct_writer(ofstream& out, t_struct* tstruct)
{
    out << indent() << "public async Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)" << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "oprot.IncrementRecursionDepth();" << endl
        << indent() << "try" << endl
        << indent() << "{" << endl;
    indent_up();

    string name = tstruct->get_name();
    const vector<t_field*>& fields = tstruct->get_sorted_members();
    vector<t_field*>::const_iterator f_iter;

    out << indent() << "var struc = new TStruct(\"" << name << "\");" << endl
        << indent() << "await oprot.WriteStructBeginAsync(struc, cancellationToken);" << endl;

    if (fields.size() > 0)
    {
        out << indent() << "var field = new TField();" << endl;
        for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
        {
            bool is_required = field_is_required(*f_iter);
            bool has_default = field_has_default(*f_iter);
            if (nullable_ && !has_default && !is_required)
            {
                out << indent() << "if (" << prop_name(*f_iter) << " != null)" << endl
                    << indent() << "{" << endl;
                indent_up();
            }
            else if (!is_required)
            {
                bool null_allowed = type_can_be_null((*f_iter)->get_type());
                if (null_allowed)
                {
                    out << indent() << "if (" << prop_name(*f_iter) << " != null && __isset." << normalize_name((*f_iter)->get_name()) << ")" << endl
                        << indent() << "{" << endl;
                    indent_up();
                }
                else
                {
                    out << indent() << "if (__isset." << normalize_name((*f_iter)->get_name()) << ")" << endl
                        << indent() << "{" << endl;
                    indent_up();
                }
            }
            out << indent() << "field.Name = \"" << (*f_iter)->get_name() << "\";" << endl
                << indent() << "field.Type = " << type_to_enum((*f_iter)->get_type()) << ";" << endl
                << indent() << "field.ID = " << (*f_iter)->get_key() << ";" << endl
                << indent() << "await oprot.WriteFieldBeginAsync(field, cancellationToken);" << endl;

            generate_serialize_field(out, *f_iter);

            out << indent() << "await oprot.WriteFieldEndAsync(cancellationToken);" << endl;
            if (!is_required)
            {
                indent_down();
                out << indent() << "}" << endl;
            }
        }
    }

    out << indent() << "await oprot.WriteFieldStopAsync(cancellationToken);" << endl
        << indent() << "await oprot.WriteStructEndAsync(cancellationToken);" << endl;
    indent_down();
    out << indent() << "}" << endl
        << indent() << "finally" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "oprot.DecrementRecursionDepth();" << endl;
    indent_down();
    out << indent() << "}" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_struct_result_writer(ofstream& out, t_struct* tstruct)
{
    out << indent() << "public async Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)" << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "oprot.IncrementRecursionDepth();" << endl
        << indent() << "try" << endl 
        << indent() << "{" << endl;
    indent_up();

    string name = tstruct->get_name();
    const vector<t_field*>& fields = tstruct->get_sorted_members();
    vector<t_field*>::const_iterator f_iter;

    out << indent() << "var struc = new TStruct(\"" << name << "\");" << endl
        << indent() << "await oprot.WriteStructBeginAsync(struc, cancellationToken);" << endl;

    if (fields.size() > 0)
    {
        out << indent() << "var field = new TField();" << endl;
        bool first = true;
        for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
        {
            if (first)
            {
                first = false;
                out << endl << indent() << "if";
            }
            else
            {
                out << indent() << "else if";
            }

            if (nullable_)
            {
                out << "(this." << prop_name((*f_iter)) << " != null)" << endl
                    << indent() << "{" << endl;
            }
            else
            {
                out << "(this.__isset." << normalize_name((*f_iter)->get_name()) << ")" << endl
                    << indent() << "{" << endl;
            }
            indent_up();

            bool null_allowed = !nullable_ && type_can_be_null((*f_iter)->get_type());
            if (null_allowed)
            {
                out << indent() << "if (" << prop_name(*f_iter) << " != null)" << endl
                    << indent() << "{" << endl;
                indent_up();
            }

            out << indent() << "field.Name = \"" << prop_name(*f_iter) << "\";" << endl
                << indent() << "field.Type = " << type_to_enum((*f_iter)->get_type()) << ";" << endl
                << indent() << "field.ID = " << (*f_iter)->get_key() << ";" << endl
                << indent() << "await oprot.WriteFieldBeginAsync(field, cancellationToken);" << endl;

            generate_serialize_field(out, *f_iter);

            out << indent() << "await oprot.WriteFieldEndAsync(cancellationToken);" << endl;

            if (null_allowed)
            {
                indent_down();
                out << indent() << "}" << endl;
            }

            indent_down();
            out << indent() << "}" << endl;
        }
    }

    out << indent() << "await oprot.WriteFieldStopAsync(cancellationToken);" << endl
        << indent() << "await oprot.WriteStructEndAsync(cancellationToken);" << endl;
    indent_down();
    out << indent() << "}" << endl
        << indent() << "finally" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "oprot.DecrementRecursionDepth();" << endl;
    indent_down();
    out << indent() << "}" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_struct_tostring(ofstream& out, t_struct* tstruct)
{
    out << indent() << "public override string ToString()" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "var sb = new StringBuilder(\"" << tstruct->get_name() << "(\");" << endl;

    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;

    bool useFirstFlag = false;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        if (!field_is_required((*f_iter)))
        {
            out << indent() << "bool __first = true;" << endl;
            useFirstFlag = true;
        }
        break;
    }

    bool had_required = false; // set to true after first required field has been processed

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        bool is_required = field_is_required((*f_iter));
        bool has_default = field_has_default((*f_iter));
        if (nullable_ && !has_default && !is_required)
        {
            out << indent() << "if (" << prop_name((*f_iter)) << " != null)" << endl
                << indent() << "{" << endl;
            indent_up();
        }
        else if (!is_required)
        {
            bool null_allowed = type_can_be_null((*f_iter)->get_type());
            if (null_allowed)
            {
                out << indent() << "if (" << prop_name((*f_iter)) << " != null && __isset." << normalize_name((*f_iter)->get_name()) << ")" << endl
                    << indent() << "{" << endl;
                indent_up();
            }
            else
            {
                out << indent() << "if (__isset." << normalize_name((*f_iter)->get_name()) << ")" << endl
                    << indent() << "{" << endl;
                indent_up();
            }
        }

        if (useFirstFlag && (!had_required))
        {
            out << indent() << "if(!__first) { sb.Append(\", \"); }" << endl;
            if (!is_required)
            {
                out << indent() << "__first = false;" << endl;
            }
            out << indent() << "sb.Append(\"" << prop_name(*f_iter) << ": \");" << endl;
        }
        else
        {
            out << indent() << "sb.Append(\", " << prop_name(*f_iter) << ": \");" << endl;
        }

        t_type* ttype = (*f_iter)->get_type();
        if (ttype->is_xception() || ttype->is_struct())
        {
            out << indent() << "sb.Append(" << prop_name(*f_iter) << "== null ? \"<null>\" : " << prop_name(*f_iter) << ".ToString());" << endl;
        }
        else
        {
            out << indent() << "sb.Append(" << prop_name(*f_iter) << ");" << endl;
        }

        if (!is_required)
        {
            indent_down();
            out << indent() << "}" << endl;
        }
        else
        {
            had_required = true; // now __first must be false, so we don't need to check it anymore
        }
    }

    out << indent() << "sb.Append(\")\");" << endl
        << indent() << "return sb.ToString();" << endl;
    indent_down();
    out << indent() << "}" << endl;
}

void t_netcore_generator::generate_netcore_union(t_struct* tunion)
{
    int ic = indent_count();

    string f_union_name = namespace_dir_ + "/" + (tunion->get_name()) + ".cs";
    ofstream f_union;

    f_union.open(f_union_name.c_str());

    f_union << autogen_comment() << netcore_type_usings() << netcore_thrift_usings() << endl;

    generate_netcore_union_definition(f_union, tunion);

    f_union.close();
    
    indent_validate(ic, "generate_netcore_union.");
}

void t_netcore_generator::generate_netcore_union_definition(ofstream& out, t_struct* tunion)
{
    // Let's define the class first
    start_netcore_namespace(out);

    out << indent() << "public abstract partial class " << tunion->get_name() << " : TAbstractBase" << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "public abstract void Write(TProtocol protocol);" << endl
        << indent() << "public readonly bool Isset;" << endl
        << indent() << "public abstract object Data { get; }" << endl
        << indent() << "protected " << tunion->get_name() << "(bool isset)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "Isset = isset;" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;

    out << indent() << "public class ___undefined : " << tunion->get_name() << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "public override object Data { get { return null; } }" << endl
        << indent() << "public ___undefined() : base(false) {}" << endl << endl;

    out << indent() << "public override void Write(TProtocol protocol)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "throw new TProtocolException( TProtocolException.INVALID_DATA, \"Cannot persist an union type which is not set.\");" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;
    indent_down();
    out << indent() << "}" << endl << endl;

    const vector<t_field*>& fields = tunion->get_members();
    vector<t_field*>::const_iterator f_iter;

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        generate_netcore_union_class(out, tunion, (*f_iter));
    }

    generate_netcore_union_reader(out, tunion);

    indent_down();
    out << indent() << "}" << endl << endl;

    end_netcore_namespace(out);
}

void t_netcore_generator::generate_netcore_union_class(ofstream& out, t_struct* tunion, t_field* tfield)
{
    out << indent() << "public class " << tfield->get_name() << " : " << tunion->get_name() << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "private " << type_name(tfield->get_type()) << " _data;" << endl
        << indent() << "public override object Data { get { return _data; } }" << endl
        << indent() << "public " << tfield->get_name() << "(" << type_name(tfield->get_type()) << " data) : base(true)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "this._data = data;" << endl;
    indent_down();
    out << indent() << "}" << endl;

    out << indent() << "public override async Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken) {" << endl;
    indent_up();

    out << indent() << "oprot.IncrementRecursionDepth();" << endl
        << indent() << "try" << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "var struc = new TStruct(\"" << tunion->get_name() << "\");" << endl
        << indent() << "await oprot.WriteStructBeginAsync(struc, cancellationToken);" << endl;

    out << indent() << "var field = new TField();" << endl
        << indent() << "field.Name = \"" << tfield->get_name() << "\";" << endl
        << indent() << "field.Type = " << type_to_enum(tfield->get_type()) << ";" << endl
        << indent() << "field.ID = " << tfield->get_key() << ";" << endl
        << indent() << "await oprot.WriteFieldBeginAsync(field, cancellationToken);" << endl;

    generate_serialize_field(out, tfield, "_data", true, true);

    out << indent() << "await oprot.WriteFieldEndAsync(cancellationToken);" << endl
        << indent() << "await oprot.WriteFieldStop(cancellationToken);" << endl
        << indent() << "await oprot.WriteStructEnd(cancellationToken);" << endl;
    indent_down();
    out << indent() << "}" << endl
        << indent() << "finally" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "oprot.DecrementRecursionDepth();" << endl;
    indent_down();
    out << indent() << "}" << endl;
    out << indent() << "}" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_struct_equals(ofstream& out, t_struct* tstruct)
{
    out << indent() << "public override bool Equals(object that)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "var other = that as " << type_name(tstruct) << ";" << endl
        << indent() << "if (other == null) return false;" << endl
        << indent() << "if (ReferenceEquals(this, other)) return true;" << endl;

    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;

    bool first = true;

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        if (first)
        {
            first = false;
            out << indent() << "return ";
            indent_up();
        }
        else
        {
            out << endl;
            out << indent() << "&& ";
        }
        if (!field_is_required((*f_iter)) && !(nullable_ && !field_has_default((*f_iter))))
        {
            out << "((__isset." << normalize_name((*f_iter)->get_name()) << " == other.__isset."
                << normalize_name((*f_iter)->get_name()) << ") && ((!__isset."
                << normalize_name((*f_iter)->get_name()) << ") || (";
        }
        t_type* ttype = (*f_iter)->get_type();
        if (ttype->is_container() || ttype->is_binary())
        {
            out << "TCollections.Equals(";
        }
        else
        {
            out << "System.Object.Equals(";
        }
        out << prop_name((*f_iter)) << ", other." << prop_name((*f_iter)) << ")";
        if (!field_is_required((*f_iter)) && !(nullable_ && !field_has_default((*f_iter))))
        {
            out << ")))";
        }
    }
    if (first)
    {
        out << indent() << "return true;" << endl;
    }
    else
    {
        out << ";" << endl;
        indent_down();
    }

    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_struct_hashcode(ofstream& out, t_struct* tstruct)
{
    out << indent() << "public override int GetHashCode() {" << endl;
    indent_up();

    out << indent() << "int hashcode = 0;" << endl;
    out << indent() << "unchecked {" << endl;
    indent_up();

    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        t_type* ttype = (*f_iter)->get_type();
        out << indent() << "hashcode = (hashcode * 397) ^ ";
        if (field_is_required((*f_iter)))
        {
            out << "(";
        }
        else if (nullable_)
        {
            out << "(" << prop_name((*f_iter)) << " == null ? 0 : ";
        }
        else
        {
            out << "(!__isset." << normalize_name((*f_iter)->get_name()) << " ? 0 : ";
        }
        if (ttype->is_container())
        {
            out << "(TCollections.GetHashCode(" << prop_name((*f_iter)) << "))";
        }
        else
        {
            out << "(" << prop_name((*f_iter)) << ".GetHashCode())";
        }
        out << ");" << endl;
    }

    indent_down();
    out << indent() << "}" << endl;
    out << indent() << "return hashcode;" << endl;

    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_service(t_service* tservice)
{
    int ic = indent_count();

    string f_service_name = namespace_dir_ + "/" + service_name_ + ".cs";
    ofstream f_service;
    f_service.open(f_service_name.c_str());

    f_service << autogen_comment() << netcore_type_usings() << netcore_thrift_usings() << endl;

    start_netcore_namespace(f_service);

    f_service << indent() << "public partial class " << normalize_name(service_name_) << endl
              << indent() << "{" << endl;
    indent_up();

    generate_service_interface(f_service, tservice);
    generate_service_client(f_service, tservice);
    generate_service_server(f_service, tservice);
    generate_service_helpers(f_service, tservice);

    indent_down();
    f_service << indent() << "}" << endl;

    end_netcore_namespace(f_service);
    f_service.close();

    indent_validate(ic, "generate_service.");
}

void t_netcore_generator::generate_service_interface(ofstream& out, t_service* tservice)
{
    string extends = "";
    string extends_iface = "";
    if (tservice->get_extends() != NULL)
    {
        extends = type_name(tservice->get_extends());
        extends_iface = " : " + extends + ".IAsync";
    }

    //out << endl << endl;

    generate_netcore_doc(out, tservice);

    if (wcf_)
    {
        out << indent() << "[ServiceContract(Namespace=\"" << wcf_namespace_ << "\")]" << endl;
    }

    out << indent() << "public interface IAsync" << extends_iface << endl
        << indent() << "{" << endl;

    indent_up();
    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::iterator f_iter;
    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter)
    {
        generate_netcore_doc(out, *f_iter);

        // if we're using WCF, add the corresponding attributes
        if (wcf_)
        {
            out << indent() << "[OperationContract]" << endl;

            const vector<t_field*>& xceptions = (*f_iter)->get_xceptions()->get_members();
            vector<t_field*>::const_iterator x_iter;
            for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter)
            {
                out << indent() << "[FaultContract(typeof(" + type_name((*x_iter)->get_type(), false, false) + "Fault))]" << endl;
            }
        }

        out << indent() << function_signature_async(*f_iter) << ";" << endl << endl;
    }
    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_service_helpers(ofstream& out, t_service* tservice)
{
    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::iterator f_iter;

    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter)
    {
        t_struct* ts = (*f_iter)->get_arglist();
        generate_netcore_struct_definition(out, ts, false, true);
        generate_function_helpers(out, *f_iter);
    }
}

void t_netcore_generator::generate_service_client(ofstream& out, t_service* tservice)
{
    string extends = "";
    string extends_client = "";
    if (tservice->get_extends() != NULL)
    {
        extends = type_name(tservice->get_extends());
        extends_client = extends + ".Client, ";
    }
    else
    {
        extends_client = "TBaseClient, IDisposable, ";
    }

    out << endl;

    generate_netcore_doc(out, tservice);

    out << indent() << "public class Client : " << extends_client << "IAsync" << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "public Client(TProtocol protocol) : this(protocol, protocol)" << endl
        << indent() << "{" << endl
        << indent() << "}" << endl
        << endl
        << indent() << "public Client(TProtocol inputProtocol, TProtocol outputProtocol) : base(inputProtocol, outputProtocol)"
        << indent() << "{" << endl
        << indent() << "}" << endl;

    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::const_iterator functions_iterator;

    for (functions_iterator = functions.begin(); functions_iterator != functions.end(); ++functions_iterator)
    {
        string function_name = correct_function_name_for_async((*functions_iterator)->get_name());

        // async
        out << indent() << "public async " << function_signature_async(*functions_iterator, "") << endl
            << indent() << "{" << endl;
        indent_up();

        string argsname = (*functions_iterator)->get_name() + "Args";

        out << indent() << "await OutputProtocol.WriteMessageBeginAsync(new TMessage(\"" << function_name
            << "\", " << ((*functions_iterator)->is_oneway() ? "TMessageType.Oneway" : "TMessageType.Call") << ", SeqId), cancellationToken);" << endl
            << indent() << endl
            << indent() << "var args = new " << argsname << "();" << endl;

        t_struct* arg_struct = (*functions_iterator)->get_arglist();
        prepare_member_name_mapping(arg_struct);
        const vector<t_field*>& fields = arg_struct->get_members();
        vector<t_field*>::const_iterator fld_iter;

        for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter)
        {
            out << indent() << "args." << prop_name(*fld_iter) << " = " << normalize_name((*fld_iter)->get_name()) << ";" << endl;
        }

        out << indent() << endl
            << indent() << "await args.WriteAsync(OutputProtocol, cancellationToken);" << endl
            << indent() << "await OutputProtocol.WriteMessageEndAsync(cancellationToken);" << endl
            << indent() << "await OutputProtocol.Transport.FlushAsync(cancellationToken);" << endl;

        if (!(*functions_iterator)->is_oneway())
        {
            string resultname = (*functions_iterator)->get_name() + "Result";
            t_struct noargs(program_);
            t_struct* xs = (*functions_iterator)->get_xceptions();
            prepare_member_name_mapping(xs, xs->get_members(), resultname);

            out << indent() << endl
                << indent() << "var msg = await InputProtocol.ReadMessageBeginAsync(cancellationToken);" << endl
                << indent() << "if (msg.Type == TMessageType.Exception)" << endl
                << indent() << "{" << endl;
            indent_up();

            out << indent() << "var x = await TApplicationException.ReadAsync(InputProtocol, cancellationToken);" << endl
                << indent() << "await InputProtocol.ReadMessageEndAsync(cancellationToken);" << endl
                << indent() << "throw x;" << endl;
            indent_down();

            out << indent() << "}" << endl 
                << endl
                << indent() << "var result = new " << resultname << "();" << endl
                << indent() << "await result.ReadAsync(InputProtocol, cancellationToken);" << endl
                << indent() << "await InputProtocol.ReadMessageEndAsync(cancellationToken);" << endl;

            if (!(*functions_iterator)->get_returntype()->is_void())
            {
                if (nullable_)
                {
                    if (type_can_be_null((*functions_iterator)->get_returntype()))
                    {
                        out << indent() << "if (result.Success != null)" << endl
                            << indent() << "{" << endl;
                        indent_up();
                        out << indent() << "return result.Success;" << endl;
                        indent_down();
                        out << indent() << "}" << endl;
                    }
                    else
                    {
                        out << indent() << "if (result.Success.HasValue)" << endl 
                            << indent() << "{" << endl;
                        indent_up();
                        out << indent() << "return result.Success.Value;" << endl;
                        indent_down();
                        out << indent() << "}" << endl;
                    }
                }
                else
                {
                    out << indent() << "if (result.__isset.success)" << endl
                        << indent() << "{" << endl;
                    indent_up();
                    out << indent() << "return result.Success;" << endl;
                    indent_down();
                    out << indent() << "}" << endl;
                }
            }

            const vector<t_field*>& xceptions = xs->get_members();
            vector<t_field*>::const_iterator x_iter;
            for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter)
            {
                if (nullable_)
                {
                    out << indent() << "if (result." << prop_name(*x_iter) << " != null)" << endl
                        << indent() << "{" << endl;
                    indent_up();
                    out << indent() << "throw result." << prop_name(*x_iter) << ";" << endl;
                    indent_down();
                    out << indent() << "}" << endl;
                }
                else
                {
                    out << indent() << "if (result.__isset." << normalize_name((*x_iter)->get_name()) << ")" << endl
                        << indent() << "{" << endl;
                    indent_up();
                    out << indent() << "throw result." << prop_name(*x_iter) << ";" << endl;
                    indent_down();
                    out << indent() << "}" << endl;
                }
            }

            if ((*functions_iterator)->get_returntype()->is_void())
            {
                out << indent() << "return;" << endl;
            }
            else
            {
                out << indent() << "throw new TApplicationException(TApplicationException.ExceptionType.MissingResult, \""
                    << function_name << " failed: unknown result\");" << endl;
            }

            cleanup_member_name_mapping((*functions_iterator)->get_xceptions());
            indent_down();
            out << indent() << "}" << endl << endl;
        }
        else
        {
            indent_down();
            out << indent() << "}" << endl;
        }
    }

    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_service_server(ofstream& out, t_service* tservice)
{
    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::iterator f_iter;

    string extends = "";
    string extends_processor = "";
    if (tservice->get_extends() != NULL)
    {
        extends = type_name(tservice->get_extends());
        extends_processor = extends + ".AsyncProcessor, ";
    }

    out << indent() << "public class AsyncProcessor : " << extends_processor << "ITAsyncProcessor" << endl
        << indent() << "{" << endl;

    indent_up();

    out << indent() << "private IAsync _iAsync;" << endl 
        << endl
        << indent() << "public AsyncProcessor(IAsync iAsync)";

    if (!extends.empty())
    {
        out << " : base(iAsync)";
    }

    out << endl
        << indent() << "{" << endl;
    indent_up();
    
    out << indent() << "if (iAsync == null) throw new ArgumentNullException(nameof(iAsync));" << endl
        << endl
        << indent() << "_iAsync = iAsync;" << endl;

    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter)
    {
        string function_name = (*f_iter)->get_name();
        out << indent() << "processMap_[\"" << correct_function_name_for_async(function_name) << "\"] = " << function_name << "_ProcessAsync;" << endl;
    }

    indent_down();
    out << indent() << "}" << endl
        << endl;

    if (extends.empty())
    {
        out << indent() << "protected delegate Task ProcessFunction(int seqid, TProtocol iprot, TProtocol oprot, CancellationToken cancellationToken);" << endl;
    }

    if (extends.empty())
    {
        out << indent() << "protected Dictionary<string, ProcessFunction> processMap_ = new Dictionary<string, ProcessFunction>();" << endl;
    }

    out << endl;

    if (extends.empty())
    {
        out << indent() << "public async Task<bool> ProcessAsync(TProtocol iprot, TProtocol oprot)" << endl
            << indent() << "{" << endl;
        indent_up();
        out << indent() << "return await ProcessAsync(iprot, oprot, CancellationToken.None);" << endl;
        indent_down();
        out << indent() << "}" << endl << endl;

        out << indent() << "public async Task<bool> ProcessAsync(TProtocol iprot, TProtocol oprot, CancellationToken cancellationToken)" << endl;
    }
    else
    {
        out << indent() << "public new async Task<bool> ProcessAsync(TProtocol iprot, TProtocol oprot)" << endl
            << indent() << "{" << endl;
        indent_up();
        out << indent() << "return await ProcessAsync(iprot, oprot, CancellationToken.None);" << endl;
        indent_down();
        out << indent() << "}" << endl << endl;

        out << indent() << "public new async Task<bool> ProcessAsync(TProtocol iprot, TProtocol oprot, CancellationToken cancellationToken)" << endl;
    }

    out << indent() << "{" << endl;
    indent_up();
    out << indent() << "try" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "var msg = await iprot.ReadMessageBeginAsync(cancellationToken);" << endl
        << endl
        << indent() << "ProcessFunction fn;" << endl
        << indent() << "processMap_.TryGetValue(msg.Name, out fn);" << endl
        << endl
        << indent() << "if (fn == null)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "await TProtocolUtil.SkipAsync(iprot, TType.Struct, cancellationToken);" << endl
        << indent() << "await iprot.ReadMessageEndAsync(cancellationToken);" << endl
        << indent() << "var x = new TApplicationException (TApplicationException.ExceptionType.UnknownMethod, \"Invalid method name: '\" + msg.Name + \"'\");" << endl
        << indent() << "await oprot.WriteMessageBeginAsync(new TMessage(msg.Name, TMessageType.Exception, msg.SeqID), cancellationToken);" << endl
        << indent() << "await x.WriteAsync(oprot, cancellationToken);" << endl
        << indent() << "await oprot.WriteMessageEndAsync(cancellationToken);" << endl
        << indent() << "await oprot.Transport.FlushAsync(cancellationToken);" << endl
        << indent() << "return true;" << endl;
    indent_down();
    out << indent() << "}" << endl
        << endl
        << indent() << "await fn(msg.SeqID, iprot, oprot, cancellationToken);" << endl
        << endl;
    indent_down();
    out << indent() << "}" << endl;
    out << indent() << "catch (IOException)" << endl
        << indent() << "{" << endl;
    indent_up();
    out << indent() << "return false;" << endl;
    indent_down();
    out << indent() << "}" << endl
        << endl
        << indent() << "return true;" << endl;
    indent_down();
    out << indent() << "}" << endl << endl;

    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter)
    {
        generate_process_function_async(out, tservice, *f_iter);
    }

    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_function_helpers(ofstream& out, t_function* tfunction)
{
    if (tfunction->is_oneway())
    {
        return;
    }

    t_struct result(program_, tfunction->get_name() + "_result");
    t_field success(tfunction->get_returntype(), "success", 0);
    if (!tfunction->get_returntype()->is_void())
    {
        result.append(&success);
    }

    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& fields = xs->get_members();
    vector<t_field*>::const_iterator f_iter;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        result.append(*f_iter);
    }

    generate_netcore_struct_definition(out, &result, false, true, true);
}

void t_netcore_generator::generate_process_function_async(ofstream& out, t_service* tservice, t_function* tfunction)
{
    (void)tservice;
    out << indent() << "public async Task " << tfunction->get_name() 
        << "_ProcessAsync(int seqid, TProtocol iprot, TProtocol oprot, CancellationToken cancellationToken)" << endl
        << indent() << "{" << endl;
    indent_up();

    string argsname = tfunction->get_name() + "Args";
    string resultname = tfunction->get_name() + "Result";

    out << indent() << "var args = new " << argsname << "();" << endl
        << indent() << "await args.ReadAsync(iprot, cancellationToken);" << endl
        << indent() << "await iprot.ReadMessageEndAsync(cancellationToken);" << endl;

    if (!tfunction->is_oneway())
    {
        out << indent() << "var result = new " << resultname << "();" << endl;
    }

    out << indent() << "try" << endl
        << indent() << "{" << endl;
    indent_up();

    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& xceptions = xs->get_members();

    if (xceptions.size() > 0)
    {
        out << indent() << "try" << endl
            << indent() << "{" << endl;
        indent_up();
    }

    t_struct* arg_struct = tfunction->get_arglist();
    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator f_iter;

    out << indent();
    if (!tfunction->is_oneway() && !tfunction->get_returntype()->is_void())
    {
        out << "result.Success = ";
    }

    out << "await _iAsync." << normalize_name(tfunction->get_name()) << "Async(";

    bool first = true;
    prepare_member_name_mapping(arg_struct);
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        if (first)
        {
            first = false;
        }
        else
        {
            out << ", ";
        }

        out << "args." << prop_name(*f_iter);
        if (nullable_ && !type_can_be_null((*f_iter)->get_type()))
        {
            out << ".Value";
        }
    }

    cleanup_member_name_mapping(arg_struct);
    
    if (!first)
    {
        out << ", ";
    }

    out << "cancellationToken);" << endl;

    vector<t_field*>::const_iterator x_iter;

    prepare_member_name_mapping(xs, xs->get_members(), resultname);
    if (xceptions.size() > 0)
    {
        indent_down();
        out << indent() << "}" << endl;
        
        for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter)
        {
            out << indent() << "catch (" << type_name((*x_iter)->get_type(), false, false) << " " << (*x_iter)->get_name() << ")" << endl
                << indent() << "{" << endl;

            if (!tfunction->is_oneway())
            {
                indent_up();
                out << indent() << "result." << prop_name(*x_iter) << " = " << (*x_iter)->get_name() << ";" << endl;
                indent_down();
            }
            out << indent() << "}" << endl;
        }
    }

    if (!tfunction->is_oneway())
    {
        out << indent() << "await oprot.WriteMessageBeginAsync(new TMessage(\"" 
                << correct_function_name_for_async(tfunction->get_name()) << "\", TMessageType.Reply, seqid), cancellationToken); " << endl
            << indent() << "await result.WriteAsync(oprot, cancellationToken);" << endl;
    }
    indent_down();

    cleanup_member_name_mapping(xs);

    out << indent() << "}" << endl
        << indent() << "catch (TTransportException)" << endl
        << indent() << "{" << endl
        << indent() << "  throw;" << endl
        << indent() << "}" << endl
        << indent() << "catch (Exception ex)" << endl
        << indent() << "{" << endl;
    indent_up();

    out << indent() << "Console.Error.WriteLine(\"Error occurred in processor:\");" << endl
        << indent() << "Console.Error.WriteLine(ex.ToString());" << endl;

    if (tfunction->is_oneway())
    {
        indent_down();
        out << indent() << "}" << endl;
    }
    else
    {
        out << indent() << "var x = new TApplicationException(TApplicationException.ExceptionType.InternalError,\" Internal error.\");" << endl
            << indent() << "await oprot.WriteMessageBeginAsync(new TMessage(\"" << correct_function_name_for_async(tfunction->get_name())
            << "\", TMessageType.Exception, seqid), cancellationToken);" << endl
            << indent() << "await x.WriteAsync(oprot, cancellationToken);" << endl;
        indent_down();

        out << indent() << "}" << endl
            << indent() << "await oprot.WriteMessageEndAsync(cancellationToken);" << endl
            << indent() << "await oprot.Transport.FlushAsync(cancellationToken);" << endl;
    }

    indent_down();
    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_netcore_union_reader(ofstream& out, t_struct* tunion)
{
    // Thanks to THRIFT-1768, we don't need to check for required fields in the union
    const vector<t_field*>& fields = tunion->get_members();
    vector<t_field*>::const_iterator f_iter;

    out << indent() << "public static " << tunion->get_name() << " Read(TProtocol iprot)" << endl;
    scope_up(out);

    out << indent() << "iprot.IncrementRecursionDepth();" << endl;
    out << indent() << "try" << endl;
    scope_up(out);

    out << indent() << tunion->get_name() << " retval;" << endl;
    out << indent() << "iprot.ReadStructBegin();" << endl;
    out << indent() << "TField field = iprot.ReadFieldBegin();" << endl;
    // we cannot have the first field be a stop -- we must have a single field defined
    out << indent() << "if (field.Type == TType.Stop)" << endl;
    scope_up(out);
    out << indent() << "iprot.ReadFieldEnd();" << endl;
    out << indent() << "retval = new ___undefined();" << endl;
    scope_down(out);
    out << indent() << "else" << endl;
    scope_up(out);
    out << indent() << "switch (field.ID)" << endl;
    scope_up(out);

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        out << indent() << "case " << (*f_iter)->get_key() << ":" << endl;
        indent_up();
        out << indent() << "if (field.Type == " << type_to_enum((*f_iter)->get_type()) << ") {" << endl;
        indent_up();

        out << indent() << type_name((*f_iter)->get_type()) << " temp;" << endl;
        generate_deserialize_field(out, (*f_iter), "temp", true);
        out << indent() << "retval = new " << (*f_iter)->get_name() << "(temp);" << endl;

        indent_down();
        out << indent() << "} else { " << endl << indent() << "  TProtocolUtil.Skip(iprot, field.Type);"
            << endl << indent() << "  retval = new ___undefined();" << endl << indent() << "}" << endl
            << indent() << "break;" << endl;
        indent_down();
    }

    out << indent() << "default: " << endl;
    indent_up();
    out << indent() << "TProtocolUtil.Skip(iprot, field.Type);" << endl << indent()
        << "retval = new ___undefined();" << endl;
    out << indent() << "break;" << endl;
    indent_down();

    scope_down(out);

    out << indent() << "iprot.ReadFieldEnd();" << endl;

    out << indent() << "if (iprot.ReadFieldBegin().Type != TType.Stop)" << endl;
    scope_up(out);
    out << indent() << "throw new TProtocolException(TProtocolException.INVALID_DATA);" << endl;
    scope_down(out);

    // end of else for TStop
    scope_down(out);
    out << indent() << "iprot.ReadStructEnd();" << endl;
    out << indent() << "return retval;" << endl;
    indent_down();

    scope_down(out);
    out << indent() << "finally" << endl;
    scope_up(out);
    out << indent() << "iprot.DecrementRecursionDepth();" << endl;
    scope_down(out);

    out << indent() << "}" << endl << endl;
}

void t_netcore_generator::generate_deserialize_field(ofstream& out, t_field* tfield, string prefix, bool is_propertyless)
{
    t_type* type = tfield->get_type();
    while (type->is_typedef())
    {
        type = static_cast<t_typedef*>(type)->get_type();
    }

    if (type->is_void())
    {
        throw "CANNOT GENERATE DESERIALIZE CODE FOR void TYPE: " + prefix + tfield->get_name();
    }

    string name = prefix + (is_propertyless ? "" : prop_name(tfield));

    if (type->is_struct() || type->is_xception())
    {
        generate_deserialize_struct(out, static_cast<t_struct*>(type), name);
    }
    else if (type->is_container())
    {
        generate_deserialize_container(out, type, name);
    }
    else if (type->is_base_type() || type->is_enum())
    {
        out << indent() << name << " = ";

        if (type->is_enum())
        {
            out << "(" << type_name(type, false, true) << ")";
        }

        out << "await iprot.";

        if (type->is_base_type())
        {
            t_base_type::t_base tbase = static_cast<t_base_type*>(type)->get_base();
            switch (tbase)
            {
            case t_base_type::TYPE_VOID:
                throw "compiler error: cannot serialize void field in a struct: " + name;
                break;
            case t_base_type::TYPE_STRING:
                if (type->is_binary())
                {
                    out << "ReadBinaryAsync(cancellationToken);";
                }
                else
                {
                    out << "ReadStringAsync(cancellationToken);";
                }
                break;
            case t_base_type::TYPE_BOOL:
                out << "ReadBoolAsync(cancellationToken);";
                break;
            case t_base_type::TYPE_I8:
                out << "ReadByteAsync(cancellationToken);";
                break;
            case t_base_type::TYPE_I16:
                out << "ReadI16Async(cancellationToken);";
                break;
            case t_base_type::TYPE_I32:
                out << "ReadI32Async(cancellationToken);";
                break;
            case t_base_type::TYPE_I64:
                out << "ReadI64Async(cancellationToken);";
                break;
            case t_base_type::TYPE_DOUBLE:
                out << "ReadDoubleAsync(cancellationToken);";
                break;
            default:
                throw "compiler error: no C# name for base type " + t_base_type::t_base_name(tbase);
            }
        }
        else if (type->is_enum())
        {
            out << "ReadI32Async(cancellationToken);";
        }
        out << endl;
    }
    else
    {
        printf("DO NOT KNOW HOW TO DESERIALIZE FIELD '%s' TYPE '%s'\n", tfield->get_name().c_str(), type_name(type).c_str());
    }
}

void t_netcore_generator::generate_deserialize_struct(ofstream& out, t_struct* tstruct, string prefix)
{
    if (union_ && tstruct->is_union())
    {
        out << indent() << prefix << " = await " << type_name(tstruct) << ".ReadAsync(iprot, cancellationToken);" << endl;
    }
    else
    {
        out << indent() << prefix << " = new " << type_name(tstruct) << "();" << endl 
            << indent() << "await " << prefix << ".ReadAsync(iprot, cancellationToken);" << endl;
    }
}

void t_netcore_generator::generate_deserialize_container(ofstream& out, t_type* ttype, string prefix)
{
    out << indent() << "{" << endl;
    indent_up();

    string obj;

    if (ttype->is_map())
    {
        obj = tmp("_map");
    }
    else if (ttype->is_set())
    {
        obj = tmp("_set");
    }
    else if (ttype->is_list())
    {
        obj = tmp("_list");
    }

    out << indent() << prefix << " = new " << type_name(ttype, false, true) << "();" << endl;
    if (ttype->is_map())
    {
        out << indent() << "TMap " << obj << " = await iprot.ReadMapBeginAsync(cancellationToken);" << endl;
    }
    else if (ttype->is_set())
    {
        out << indent() << "TSet " << obj << " = await iprot.ReadSetBeginAsync(cancellationToken);" << endl;
    }
    else if (ttype->is_list())
    {
        out << indent() << "TList " << obj << " = await iprot.ReadListBeginAsync(cancellationToken);" << endl;
    }

    string i = tmp("_i");
    out << indent() << "for(int " << i << " = 0; " << i << " < " << obj << ".Count; ++" << i << ")" << endl
        << indent() << "{" << endl;
    indent_up();

    if (ttype->is_map())
    {
        generate_deserialize_map_element(out, static_cast<t_map*>(ttype), prefix);
    }
    else if (ttype->is_set())
    {
        generate_deserialize_set_element(out, static_cast<t_set*>(ttype), prefix);
    }
    else if (ttype->is_list())
    {
        generate_deserialize_list_element(out, static_cast<t_list*>(ttype), prefix);
    }

    indent_down();
    out << indent() << "}" << endl;

    if (ttype->is_map())
    {
        out << indent() << "await iprot.ReadMapEndAsync(cancellationToken);" << endl;
    }
    else if (ttype->is_set())
    {
        out << indent() << "await iprot.ReadSetEndAsync(cancellationToken);" << endl;
    }
    else if (ttype->is_list())
    {
        out << indent() << "await iprot.ReadListEndAsync(cancellationToken);" << endl;
    }

    indent_down();
    out << indent() << "}" << endl;
}

void t_netcore_generator::generate_deserialize_map_element(ofstream& out, t_map* tmap, string prefix)
{
    string key = tmp("_key");
    string val = tmp("_val");

    t_field fkey(tmap->get_key_type(), key);
    t_field fval(tmap->get_val_type(), val);

    out << indent() << declare_field(&fkey) << endl;
    out << indent() << declare_field(&fval) << endl;

    generate_deserialize_field(out, &fkey);
    generate_deserialize_field(out, &fval);

    out << indent() << prefix << "[" << key << "] = " << val << ";" << endl;
}

void t_netcore_generator::generate_deserialize_set_element(ofstream& out, t_set* tset, string prefix)
{
    string elem = tmp("_elem");
    t_field felem(tset->get_elem_type(), elem);

    out << indent() << declare_field(&felem) << endl;

    generate_deserialize_field(out, &felem);

    out << indent() << prefix << ".Add(" << elem << ");" << endl;
}

void t_netcore_generator::generate_deserialize_list_element(ofstream& out, t_list* tlist, string prefix)
{
    string elem = tmp("_elem");
    t_field felem(tlist->get_elem_type(), elem);

    out << indent() << declare_field(&felem) << endl;

    generate_deserialize_field(out, &felem);

    out << indent() << prefix << ".Add(" << elem << ");" << endl;
}

void t_netcore_generator::generate_serialize_field(ofstream& out, t_field* tfield, string prefix, bool is_element, bool is_propertyless)
{
    t_type* type = tfield->get_type();
    while (type->is_typedef())
    {
        type = static_cast<t_typedef*>(type)->get_type();
    }

    string name = prefix + (is_propertyless ? "" : prop_name(tfield));

    if (type->is_void())
    {
        throw "CANNOT GENERATE SERIALIZE CODE FOR void TYPE: " + name;
    }

    if (type->is_struct() || type->is_xception())
    {
        generate_serialize_struct(out, static_cast<t_struct*>(type), name);
    }
    else if (type->is_container())
    {
        generate_serialize_container(out, type, name);
    }
    else if (type->is_base_type() || type->is_enum())
    {
        out << indent() << "await oprot.";

        string nullable_name = nullable_ && !is_element && !field_is_required(tfield) ? name + ".Value" : name;

        if (type->is_base_type())
        {
            t_base_type::t_base tbase = static_cast<t_base_type*>(type)->get_base();
            switch (tbase)
            {
            case t_base_type::TYPE_VOID:
                throw "compiler error: cannot serialize void field in a struct: " + name;
            case t_base_type::TYPE_STRING:
                if (type->is_binary())
                {
                    out << "WriteBinaryAsync(";
                }
                else
                {
                    out << "WriteStringAsync(";
                }
                out << name << ", cancellationToken);";
                break;
            case t_base_type::TYPE_BOOL:
                out << "WriteBoolAsync(" << nullable_name << ", cancellationToken);";
                break;
            case t_base_type::TYPE_I8:
                out << "WriteByteAsync(" << nullable_name << ", cancellationToken);";
                break;
            case t_base_type::TYPE_I16:
                out << "WriteI16Async(" << nullable_name << ", cancellationToken);";
                break;
            case t_base_type::TYPE_I32:
                out << "WriteI32Async(" << nullable_name << ", cancellationToken);";
                break;
            case t_base_type::TYPE_I64:
                out << "WriteI64Async(" << nullable_name << ", cancellationToken);";
                break;
            case t_base_type::TYPE_DOUBLE:
                out << "WriteDoubleAsync(" << nullable_name << ", cancellationToken);";
                break;
            default:
                throw "compiler error: no C# name for base type " + t_base_type::t_base_name(tbase);
            }
        }
        else if (type->is_enum())
        {
            out << "WriteI32Async((int)" << nullable_name << ", cancellationToken);";
        }
        out << endl;
    }
    else
    {
        printf("DO NOT KNOW HOW TO SERIALIZE '%s%s' TYPE '%s'\n", prefix.c_str(), tfield->get_name().c_str(), type_name(type).c_str());
    }
}

void t_netcore_generator::generate_serialize_struct(ofstream& out, t_struct* tstruct, string prefix)
{
    (void)tstruct;
    out << indent() << "await " << prefix << ".WriteAsync(oprot, cancellationToken);" << endl;
}

void t_netcore_generator::generate_serialize_container(ofstream& out, t_type* ttype, string prefix)
{
    out << indent() << "{" << endl;
    indent_up();

    if (ttype->is_map())
    {
        out << indent() << "await oprot.WriteMapBeginAsync(new TMap(" << type_to_enum(static_cast<t_map*>(ttype)->get_key_type())
            << ", " << type_to_enum(static_cast<t_map*>(ttype)->get_val_type()) << ", " << prefix
            << ".Count), cancellationToken);" << endl;
    }
    else if (ttype->is_set())
    {
        out << indent() << "await oprot.WriteSetBeginAsync(new TSet(" << type_to_enum(static_cast<t_set*>(ttype)->get_elem_type())
            << ", " << prefix << ".Count), cancellationToken);" << endl;
    }
    else if (ttype->is_list())
    {
        out << indent() << "await oprot.WriteListBeginAsync(new TList("
            << type_to_enum(static_cast<t_list*>(ttype)->get_elem_type()) << ", " << prefix << ".Count), cancellationToken);"
            << endl;
    }

    string iter = tmp("_iter");
    if (ttype->is_map())
    {
        out << indent() << "foreach (" << type_name(static_cast<t_map*>(ttype)->get_key_type()) << " " << iter
            << " in " << prefix << ".Keys)";
    }
    else if (ttype->is_set())
    {
        out << indent() << "foreach (" << type_name(static_cast<t_set*>(ttype)->get_elem_type()) << " " << iter
            << " in " << prefix << ")";
    }
    else if (ttype->is_list())
    {
        out << indent() << "foreach (" << type_name(static_cast<t_list*>(ttype)->get_elem_type()) << " " << iter
            << " in " << prefix << ")";
    }

    out << endl;
    out << indent() << "{" << endl;
    indent_up();

    if (ttype->is_map())
    {
        generate_serialize_map_element(out, static_cast<t_map*>(ttype), iter, prefix);
    }
    else if (ttype->is_set())
    {
        generate_serialize_set_element(out, static_cast<t_set*>(ttype), iter);
    }
    else if (ttype->is_list())
    {
        generate_serialize_list_element(out, static_cast<t_list*>(ttype), iter);
    }

    indent_down();
    out << indent() << "}" << endl;

    if (ttype->is_map())
    {
        out << indent() << "await oprot.WriteMapEndAsync(cancellationToken);" << endl;
    }
    else if (ttype->is_set())
    {
        out << indent() << "await oprot.WriteSetEndAsync(cancellationToken);" << endl;
    }
    else if (ttype->is_list())
    {
        out << indent() << "await oprot.WriteListEndAsync(cancellationToken);" << endl;
    }

    indent_down();
    out << indent() << "}" << endl;
}

void t_netcore_generator::generate_serialize_map_element(ofstream& out, t_map* tmap, string iter, string map)
{
    t_field kfield(tmap->get_key_type(), iter);
    generate_serialize_field(out, &kfield, "", true);
    t_field vfield(tmap->get_val_type(), map + "[" + iter + "]");
    generate_serialize_field(out, &vfield, "", true);
}

void t_netcore_generator::generate_serialize_set_element(ofstream& out, t_set* tset, string iter)
{
    t_field efield(tset->get_elem_type(), iter);
    generate_serialize_field(out, &efield, "", true);
}

void t_netcore_generator::generate_serialize_list_element(ofstream& out, t_list* tlist, string iter)
{
    t_field efield(tlist->get_elem_type(), iter);
    generate_serialize_field(out, &efield, "", true);
}

void t_netcore_generator::generate_property(ofstream& out, t_field* tfield, bool isPublic, bool generateIsset)
{
    generate_netcore_property(out, tfield, isPublic, generateIsset, "_");
}

void t_netcore_generator::generate_netcore_property(ofstream& out, t_field* tfield, bool isPublic, bool generateIsset, string fieldPrefix)
{
    if ((serialize_ || wcf_) && isPublic)
    {
        out << indent() << "[DataMember(Order = 0)]" << endl;
    }
    bool has_default = field_has_default(tfield);
    bool is_required = field_is_required(tfield);
    if ((nullable_ && !has_default) || is_required)
    {
        out << indent() << (isPublic ? "public " : "private ") << type_name(tfield->get_type(), false, false, true, is_required) << " " << prop_name(tfield) << " { get; set; }" << endl;
    }
    else
    {
        out << indent() << (isPublic ? "public " : "private ")  << type_name(tfield->get_type(), false, false, true) << " " << prop_name(tfield) << endl
            << indent() << "{" << endl;
        indent_up();

        out << indent() << "get" << endl
            << indent() << "{" << endl;
        indent_up();

        bool use_nullable = false;
        if (nullable_)
        {
            t_type* ttype = tfield->get_type();
            while (ttype->is_typedef())
            {
                ttype = static_cast<t_typedef*>(ttype)->get_type();
            }
            if (ttype->is_base_type())
            {
                use_nullable = static_cast<t_base_type*>(ttype)->get_base() != t_base_type::TYPE_STRING;
            }
        }

        out << indent() << "return " << fieldPrefix + tfield->get_name() << ";" << endl;
        indent_down();
        out << indent() << "}" << endl
            << indent() << "set" << endl
            << indent() << "{" << endl;
        indent_up();

        if (use_nullable)
        {
            if (generateIsset)
            {
                out << indent() << "__isset." << normalize_name(tfield->get_name()) << " = value.HasValue;" << endl;
            }
            out << indent() << "if (value.HasValue) this." << fieldPrefix + tfield->get_name() << " = value.Value;" << endl;
        }
        else
        {
            if (generateIsset)
            {
                out << indent() << "__isset." << normalize_name(tfield->get_name()) << " = true;" << endl;
            }
            out << indent() << "this." << fieldPrefix + tfield->get_name() << " = value;" << endl;
        }

        indent_down();
        out << indent() << "}" << endl;
        indent_down();
        out << indent() << "}" << endl;
    }
    out << endl;
}

string t_netcore_generator::make_valid_csharp_identifier(string const& fromName)
{
    string str = fromName;
    if (str.empty())
    {
        return str;
    }

    // tests rely on this
    assert(('A' < 'Z') && ('a' < 'z') && ('0' < '9'));

    // if the first letter is a number, we add an additional underscore in front of it
    char c = str.at(0);
    if (('0' <= c) && (c <= '9'))
    {
        str = "_" + str;
    }

    // following chars: letter, number or underscore
    for (size_t i = 0; i < str.size(); ++i)
    {
        c = str.at(i);
        if (('A' > c || c > 'Z') && ('a' > c || c > 'z') && ('0' > c || c > '9') && '_' != c)
        {
            str.replace(i, 1, "_");
        }
    }

    return str;
}

void t_netcore_generator::cleanup_member_name_mapping(void* scope)
{
    if (member_mapping_scopes.empty())
    {
        throw "internal error: cleanup_member_name_mapping() no scope active";
    }

    member_mapping_scope& active = member_mapping_scopes.back();
    if (active.scope_member != scope)
    {
        throw "internal error: cleanup_member_name_mapping() called for wrong struct";
    }

    member_mapping_scopes.pop_back();
}

string t_netcore_generator::get_mapped_member_name(string name)
{
    if (!member_mapping_scopes.empty())
    {
        member_mapping_scope& active = member_mapping_scopes.back();
        map<string, string>::iterator iter = active.mapping_table.find(name);
        if (active.mapping_table.end() != iter)
        {
            return iter->second;
        }
    }

    pverbose("no mapping for member %s\n", name.c_str());
    return name;
}

void t_netcore_generator::prepare_member_name_mapping(t_struct* tstruct)
{
    prepare_member_name_mapping(tstruct, tstruct->get_members(), tstruct->get_name());
}

void t_netcore_generator::prepare_member_name_mapping(void* scope, const vector<t_field*>& members, const string& structname)
{
    // begin new scope
    member_mapping_scope dummy;
    dummy.scope_member = 0;
    member_mapping_scopes.push_back(dummy);
    member_mapping_scope& active = member_mapping_scopes.back();
    active.scope_member = scope;

    // current C# generator policy:
    // - prop names are always rendered with an Uppercase first letter
    // - struct names are used as given
    std::set<string> used_member_names;
    vector<t_field*>::const_iterator iter;

    // prevent name conflicts with struct (CS0542 error)
    used_member_names.insert(structname);

    // prevent name conflicts with known methods (THRIFT-2942)
    used_member_names.insert("Read");
    used_member_names.insert("Write");

    for (iter = members.begin(); iter != members.end(); ++iter)
    {
        string oldname = (*iter)->get_name();
        string newname = prop_name(*iter, true);
        while (true)
        {
            // new name conflicts with another member
            if (used_member_names.find(newname) != used_member_names.end())
            {
                pverbose("struct %s: member %s conflicts with another member\n", structname.c_str(), newname.c_str());
                newname += '_';
                continue;
            }

            // add always, this helps us to detect edge cases like
            // different spellings ("foo" and "Foo") within the same struct
            pverbose("struct %s: member mapping %s => %s\n", structname.c_str(), oldname.c_str(), newname.c_str());
            active.mapping_table[oldname] = newname;
            used_member_names.insert(newname);
            break;
        }
    }
}

string t_netcore_generator::prop_name(t_field* tfield, bool suppress_mapping)
{
    string name(tfield->get_name());
    if (suppress_mapping)
    {
        name[0] = toupper(name[0]);
    }
    else
    {
        name = get_mapped_member_name(name);
    }
    return name;
}

string t_netcore_generator::type_name(t_type* ttype, bool in_container, bool in_init, bool in_param, bool is_required)
{
    (void)in_init;

    while (ttype->is_typedef())
    {
        ttype = static_cast<t_typedef*>(ttype)->get_type();
    }

    if (ttype->is_base_type())
    {
        return base_type_name(static_cast<t_base_type*>(ttype), in_container, in_param, is_required);
    }

    if (ttype->is_map())
    {
        t_map* tmap = static_cast<t_map*>(ttype);
        return "Dictionary<" + type_name(tmap->get_key_type(), true) + ", " + type_name(tmap->get_val_type(), true) + ">";
    }

    if (ttype->is_set())
    {
        t_set* tset = static_cast<t_set*>(ttype);
        return "THashSet<" + type_name(tset->get_elem_type(), true) + ">";
    }

    if (ttype->is_list())
    {
        t_list* tlist = static_cast<t_list*>(ttype);
        return "List<" + type_name(tlist->get_elem_type(), true) + ">";
    }

    t_program* program = ttype->get_program();
    string postfix = (!is_required && nullable_ && in_param && ttype->is_enum()) ? "?" : "";
    if (program != NULL && program != program_)
    {
        string ns = program->get_namespace("netcore");
        if (!ns.empty())
        {
            return ns + "." + normalize_name(ttype->get_name()) + postfix;
        }
    }

    return normalize_name(ttype->get_name()) + postfix;
}

string t_netcore_generator::base_type_name(t_base_type* tbase, bool in_container, bool in_param, bool is_required)
{
    (void)in_container;
    string postfix = (!is_required && nullable_ && in_param) ? "?" : "";
    switch (tbase->get_base())
    {
    case t_base_type::TYPE_VOID:
        return "void";
    case t_base_type::TYPE_STRING:
        {
            if (tbase->is_binary())
            {
                return "byte[]";
            }
            return "string";
        }
    case t_base_type::TYPE_BOOL:
        return "bool" + postfix;
    case t_base_type::TYPE_I8:
        return "sbyte" + postfix;
    case t_base_type::TYPE_I16:
        return "short" + postfix;
    case t_base_type::TYPE_I32:
        return "int" + postfix;
    case t_base_type::TYPE_I64:
        return "long" + postfix;
    case t_base_type::TYPE_DOUBLE:
        return "double" + postfix;
    default:
        throw "compiler error: no C# name for base type " + t_base_type::t_base_name(tbase->get_base());
    }
}

string t_netcore_generator::declare_field(t_field* tfield, bool init, string prefix)
{
    string result = type_name(tfield->get_type()) + " " + prefix + tfield->get_name();
    if (init)
    {
        t_type* ttype = tfield->get_type();
        while (ttype->is_typedef())
        {
            ttype = static_cast<t_typedef*>(ttype)->get_type();
        }
        if (ttype->is_base_type() && field_has_default(tfield))
        {
            ofstream dummy;
            result += " = " + render_const_value(dummy, tfield->get_name(), ttype, tfield->get_value());
        }
        else if (ttype->is_base_type())
        {
            t_base_type::t_base tbase = static_cast<t_base_type*>(ttype)->get_base();
            switch (tbase)
            {
            case t_base_type::TYPE_VOID:
                throw "NO T_VOID CONSTRUCT";
            case t_base_type::TYPE_STRING:
                result += " = null";
                break;
            case t_base_type::TYPE_BOOL:
                result += " = false";
                break;
            case t_base_type::TYPE_I8:
            case t_base_type::TYPE_I16:
            case t_base_type::TYPE_I32:
            case t_base_type::TYPE_I64:
                result += " = 0";
                break;
            case t_base_type::TYPE_DOUBLE:
                result += " = (double)0";
                break;
            }
        }
        else if (ttype->is_enum())
        {
            result += " = (" + type_name(ttype, false, true) + ")0";
        }
        else if (ttype->is_container())
        {
            result += " = new " + type_name(ttype, false, true) + "()";
        }
        else
        {
            result += " = new " + type_name(ttype, false, true) + "()";
        }
    }
    return result + ";";
}

string t_netcore_generator::function_signature(t_function* tfunction, string prefix)
{
    t_type* ttype = tfunction->get_returntype();
    return type_name(ttype) + " " + normalize_name(prefix + tfunction->get_name()) + "(" + argument_list(tfunction->get_arglist()) + ")";
}

string t_netcore_generator::function_signature_async(t_function* tfunction, string prefix)
{
    t_type* ttype = tfunction->get_returntype();
    string task = "Task";
    if (!ttype->is_void())
    {
        task += "<" + type_name(ttype) + ">";
    }

    string result = task + " " + normalize_name(prefix + tfunction->get_name()) + "Async(";
    string args = argument_list(tfunction->get_arglist());
    result += args;
    if (!args.empty())
    {
        result += ", ";
    }
    result += "CancellationToken cancellationToken)";

    return result;
}

string t_netcore_generator::argument_list(t_struct* tstruct)
{
    string result = "";
    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;
    bool first = true;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter)
    {
        if (first)
        {
            first = false;
        }
        else
        {
            result += ", ";
        }
        result += type_name((*f_iter)->get_type()) + " " + normalize_name((*f_iter)->get_name());
    }
    return result;
}

string t_netcore_generator::type_to_enum(t_type* type)
{
    while (type->is_typedef())
    {
        type = static_cast<t_typedef*>(type)->get_type();
    }

    if (type->is_base_type())
    {
        t_base_type::t_base tbase = static_cast<t_base_type*>(type)->get_base();
        switch (tbase)
        {
        case t_base_type::TYPE_VOID:
            throw "NO T_VOID CONSTRUCT";
        case t_base_type::TYPE_STRING:
            return "TType.String";
        case t_base_type::TYPE_BOOL:
            return "TType.Bool";
        case t_base_type::TYPE_I8:
            return "TType.Byte";
        case t_base_type::TYPE_I16:
            return "TType.I16";
        case t_base_type::TYPE_I32:
            return "TType.I32";
        case t_base_type::TYPE_I64:
            return "TType.I64";
        case t_base_type::TYPE_DOUBLE:
            return "TType.Double";
        }
    }
    else if (type->is_enum())
    {
        return "TType.I32";
    }
    else if (type->is_struct() || type->is_xception())
    {
        return "TType.Struct";
    }
    else if (type->is_map())
    {
        return "TType.Map";
    }
    else if (type->is_set())
    {
        return "TType.Set";
    }
    else if (type->is_list())
    {
        return "TType.List";
    }

    throw "INVALID TYPE IN type_to_enum: " + type->get_name();
}

void t_netcore_generator::generate_netcore_docstring_comment(ofstream& out, string contents)
{
    docstring_comment(out, "/// <summary>" + endl, "/// ", contents, "/// </summary>" + endl);
}

void t_netcore_generator::generate_netcore_doc(ofstream& out, t_field* field)
{
    if (field->get_type()->is_enum())
    {
        string combined_message = field->get_doc() + endl + "<seealso cref=\"" + get_enum_class_name(field->get_type()) + "\"/>";
        generate_netcore_docstring_comment(out, combined_message);
    }
    else
    {
        generate_netcore_doc(out, static_cast<t_doc*>(field));
    }
}

void t_netcore_generator::generate_netcore_doc(ofstream& out, t_doc* tdoc)
{
    if (tdoc->has_doc())
    {
        generate_netcore_docstring_comment(out, tdoc->get_doc());
    }
}

void t_netcore_generator::generate_netcore_doc(ofstream& out, t_function* tfunction)
{
    if (tfunction->has_doc())
    {
        stringstream ps;
        const vector<t_field*>& fields = tfunction->get_arglist()->get_members();
        vector<t_field*>::const_iterator p_iter;
        for (p_iter = fields.begin(); p_iter != fields.end(); ++p_iter)
        {
            t_field* p = *p_iter;
            ps << endl << "<param name=\"" << p->get_name() << "\">";
            if (p->has_doc())
            {
                string str = p->get_doc();
                str.erase(remove(str.begin(), str.end(), '\n'), str.end());
                ps << str;
            }
            ps << "</param>";
        }

        docstring_comment(out,
                                   "",
                                   "/// ",
                                   "<summary>" + endl + tfunction->get_doc() + "</summary>" + ps.str(),
                                   "");
    }
}

void t_netcore_generator::docstring_comment(ofstream& out, const string& comment_start, const string& line_prefix, const string& contents, const string& comment_end)
{
    if (comment_start != "")
    {
        out << indent() << comment_start;
    }

    stringstream docs(contents, std::ios_base::in);

    while (!(docs.eof() || docs.fail())) 
    {
        char line[1024];
        docs.getline(line, 1024);

        // Just prnt a newline when the line & prefix are empty.
        if (strlen(line) == 0 && line_prefix == "" && !docs.eof()) 
        {
            out << endl;
        }
        else if (strlen(line) > 0 || !docs.eof()) 
        { // skip the empty last line
            out << indent() << line_prefix << line << endl;
        }
    }
    if (comment_end != "")
    {
        out << indent() << comment_end;
    }
}

string t_netcore_generator::get_enum_class_name(t_type* type)
{
    string package = "";
    t_program* program = type->get_program();
    if (program != NULL && program != program_)
    {
        package = program->get_namespace("netcore") + ".";
    }
    return package + type->get_name();
}

THRIFT_REGISTER_GENERATOR(
    netcore,
    "C#",
    "    wcf:             Adds bindings for WCF to generated classes.\n"
    "    serial:          Add serialization support to generated classes.\n"
    "    nullable:        Use nullable types for properties.\n"
    "    hashcode:        Generate a hashcode and equals implementation for classes.\n"
    "    union:           Use new union typing, which includes a static read function for union types.\n"
)
