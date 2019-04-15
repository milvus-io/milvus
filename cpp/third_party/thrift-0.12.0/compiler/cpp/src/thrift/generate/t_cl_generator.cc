/*
 * Copyright (c) 2008- Patrick Collison <patrick@collison.ie>
 * Copyright (c) 2006- Facebook
 *
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
 */

#include <string>
#include <fstream>
#include <iostream>
#include <vector>

#include <stdlib.h>
#include <boost/tokenizer.hpp>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <string>
#include <algorithm>

#include "thrift/platform.h"
#include "t_oop_generator.h"
using namespace std;


/**
 * Common Lisp code generator.
 *
 * @author Patrick Collison <patrick@collison.ie>
 */
class t_cl_generator : public t_oop_generator {
 public:
  t_cl_generator(
      t_program* program,
      const std::map<std::string, std::string>& parsed_options,
      const std::string& option_string)
    : t_oop_generator(program)
  {
    no_asd = false;
    system_prefix = "thrift-gen-";

    std::map<std::string, std::string>::const_iterator iter;

    for(iter = parsed_options.begin(); iter != parsed_options.end(); ++iter) {
      if(iter->first.compare("no_asd") == 0) {
        no_asd = true;
      } else if (iter->first.compare("sys_pref") == 0) {
	system_prefix = iter->second;
      } else {
        throw "unknown option cl:" + iter->first;
      }
    }

    out_dir_base_ = "gen-cl";
    copy_options_ = option_string;
  }

  void init_generator();
  void close_generator();

  void generate_typedef     (t_typedef*  ttypedef);
  void generate_enum        (t_enum*     tenum);
  void generate_const       (t_const*    tconst);
  void generate_struct      (t_struct*   tstruct);
  void generate_xception    (t_struct*   txception);
  void generate_service     (t_service*  tservice);
  void generate_cl_struct (std::ostream& out, t_struct* tstruct, bool is_exception);
  void generate_cl_struct_internal (std::ostream& out, t_struct* tstruct, bool is_exception);
  void generate_exception_sig(std::ostream& out, t_function* f);
  std::string render_const_value(t_type* type, t_const_value* value);

  std::string cl_autogen_comment();
  void asdf_def(std::ostream &out);
  void package_def(std::ostream &out);
  void package_in(std::ostream &out);
  std::string generated_package();
  std::string prefix(std::string name);
  std::string package_of(t_program* program);
  std::string package();
  std::string render_includes();

  std::string type_name(t_type* ttype);
  std::string typespec (t_type *t);
  std::string function_signature(t_function* tfunction);
  std::string argument_list(t_struct* tstruct);

  std::string cl_docstring(std::string raw);

 private:

  int temporary_var;
  /**
   * Isolate the variable definitions, as they can require structure definitions
   */
  ofstream_with_content_based_conditional_update f_asd_;
  ofstream_with_content_based_conditional_update f_types_;
  ofstream_with_content_based_conditional_update f_vars_;

  std::string copy_options_;

  bool no_asd;
  std::string system_prefix;
};


void t_cl_generator::init_generator() {
  MKDIR(get_out_dir().c_str());
  string program_dir = get_out_dir() + "/" + program_name_;
  MKDIR(program_dir.c_str());

  temporary_var = 0;

  string f_types_name = program_dir + "/" + program_name_ + "-types.lisp";
  string f_vars_name = program_dir + "/" + program_name_ + "-vars.lisp";

  f_types_.open(f_types_name.c_str());
  f_types_ << cl_autogen_comment() << endl;
  f_vars_.open(f_vars_name.c_str());
  f_vars_ << cl_autogen_comment() << endl;

  package_def(f_types_);
  package_in(f_types_);
  package_in(f_vars_);

  if (!no_asd) {
    string f_asd_name = program_dir + "/" + system_prefix + program_name_ + ".asd";
    f_asd_.open(f_asd_name.c_str());
    f_asd_ << cl_autogen_comment() << endl;
    asdf_def(f_asd_);
  }
}

/**
 * Renders all the imports necessary for including another Thrift program
 */
string t_cl_generator::render_includes() {
  const vector<t_program*>& includes = program_->get_includes();
  string result = "";
  result += ":depends-on (:thrift";
  for (size_t i = 0; i < includes.size(); ++i) {
    result += " :" + system_prefix + underscore(includes[i]->get_name());
  }
  result += ")\n";
  return result;
}

string t_cl_generator::package_of(t_program* program) {
  string prefix = program->get_namespace("cl");
  return prefix.empty() ? "thrift-generated" : prefix;
}

string t_cl_generator::package() {
  return package_of(program_);
}

string t_cl_generator::prefix(string symbol) {
  return "\"" + symbol + "\"";
}

string t_cl_generator::cl_autogen_comment() {
  return
    std::string(";;; ") + "Autogenerated by Thrift\n" +
    ";;; DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING\n" +
    ";;; options string: " + copy_options_ + "\n";
}

string t_cl_generator::cl_docstring(string raw) {
  replace(raw.begin(), raw.end(), '"', '\'');
  return raw;
}


void t_cl_generator::close_generator() {
  f_asd_.close();
  f_types_.close();
  f_vars_.close();
}

string t_cl_generator::generated_package() {
  return program_->get_namespace("cpp");
}

void t_cl_generator::asdf_def(std::ostream &out) {
  out << "(asdf:defsystem #:" << system_prefix << program_name_ << endl;
  indent_up();
  out << indent() << render_includes()
      << indent() << ":serial t" << endl
      << indent() << ":components ("
      << "(:file \"" << program_name_ << "-types\") "
      << "(:file \"" << program_name_ << "-vars\")))" << endl;
  indent_down();
}

/***
 * Generate a package definition. Add use references equivalent to the idl file's include statements.
 */
void t_cl_generator::package_def(std::ostream &out) {
  const vector<t_program*>& includes = program_->get_includes();

  out << "(thrift:def-package :" << package();
  if ( includes.size() > 0 ) {
    out << " :use (";
    for (size_t i = 0; i < includes.size(); ++i) {
      out << " :" << includes[i]->get_name();
    }
    out << ")";
  }
  out << ")" << endl << endl;
}

void t_cl_generator::package_in(std::ostream &out) {
  out << "(cl:in-package :" << package() << ")" << endl << endl;
}

/**
 * Generates a typedef. This is not done in Common Lisp, types are all implicit.
 *
 * @param ttypedef The type definition
 */
void t_cl_generator::generate_typedef(t_typedef* ttypedef) {
  (void)ttypedef;
}

void t_cl_generator::generate_enum(t_enum* tenum) {
  f_types_ << "(thrift:def-enum " << prefix(tenum->get_name()) << endl;

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;
  int value = -1;

  indent_up();
  f_types_ << indent() << "(";
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    value = (*c_iter)->get_value();

    if(c_iter != constants.begin()) f_types_ << endl << indent() << " ";

    f_types_ << "(\"" << (*c_iter)->get_name() << "\" . " << value << ")";
  }
  indent_down();
  f_types_ << "))" << endl << endl;
}

/**
 * Generate a constant value
 */
void t_cl_generator::generate_const(t_const* tconst) {
  t_type* type = tconst->get_type();
  string name = tconst->get_name();
  t_const_value* value = tconst->get_value();

  f_vars_ << "(thrift:def-constant " << prefix(name) << " " << render_const_value(type, value) << ")"
          << endl << endl;
}

/**
 * Prints the value of a constant with the given type. Note that type checking
 * is NOT performed in this function as it is always run beforehand using the
 * validate_types method in main.cc
 */
string t_cl_generator::render_const_value(t_type* type, t_const_value* value) {
  type = get_true_type(type);
  std::ostringstream out;
  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_STRING:
      out << "\"" << value->get_string() << "\"";
      break;
    case t_base_type::TYPE_BOOL:
      out << (value->get_integer() > 0 ? "t" : "nil");
      break;
    case t_base_type::TYPE_I8:
    case t_base_type::TYPE_I16:
    case t_base_type::TYPE_I32:
    case t_base_type::TYPE_I64:
      out << value->get_integer();
      break;
    case t_base_type::TYPE_DOUBLE:
      if (value->get_type() == t_const_value::CV_INTEGER) {
        out << value->get_integer();
      } else {
        out << value->get_double();
      }
      break;
    default:
      throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
    }
  } else if (type->is_enum()) {
    indent(out) << value->get_integer();
  } else if (type->is_struct() || type->is_xception()) {
    out << (type->is_struct() ? "(make-instance '" : "(make-exception '") <<
           lowercase(type->get_name()) << " " << endl;
    indent_up();

    const vector<t_field*>& fields = ((t_struct*)type)->get_members();
    vector<t_field*>::const_iterator f_iter;
    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;

    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      t_type* field_type = NULL;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if ((*f_iter)->get_name() == v_iter->first->get_string()) {
          field_type = (*f_iter)->get_type();
        }
      }
      if (field_type == NULL) {
        throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
      }

      out << indent() << ":" << v_iter->first->get_string() << " " <<
        render_const_value(field_type, v_iter->second) << endl;
    }
    out << indent() << ")";

    indent_down();
  } else if (type->is_map()) {
    // emit an hash form with both keys and values to be evaluated
    t_type* ktype = ((t_map*)type)->get_key_type();
    t_type* vtype = ((t_map*)type)->get_val_type();
    out << "(thrift:map ";
    indent_up();
    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      out << endl << indent()
          << "(cl:cons " << render_const_value(ktype, v_iter->first) << " "
          << render_const_value(vtype, v_iter->second) << ")";
    }
    indent_down();
    out << indent() << ")";
  } else if (type->is_list() || type->is_set()) {
    t_type* etype;
    if (type->is_list()) {
      etype = ((t_list*)type)->get_elem_type();
    } else {
      etype = ((t_set*)type)->get_elem_type();
    }
    if (type->is_set()) {
      out << "(thrift:set" << endl;
    } else {
      out << "(thrift:list" << endl;
    }
    indent_up();
    indent_up();
    const vector<t_const_value*>& val = value->get_list();
    vector<t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      out << indent() << render_const_value(etype, *v_iter) << endl;
    }
    out << indent() << ")";
    indent_down();
    indent_down();
  } else {
    throw "CANNOT GENERATE CONSTANT FOR TYPE: " + type->get_name();
  }
  return out.str();
}

void t_cl_generator::generate_struct(t_struct* tstruct) {
  generate_cl_struct(f_types_, tstruct, false);
}

void t_cl_generator::generate_xception(t_struct* txception) {
  generate_cl_struct(f_types_, txception, true);
}

void t_cl_generator::generate_cl_struct_internal(std::ostream& out, t_struct* tstruct, bool is_exception) {
  (void)is_exception;
  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  out << "(";

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    t_const_value* value = (*m_iter)->get_value();
    t_type* type = (*m_iter)->get_type();

    if (m_iter != members.begin()) {
      out << endl << indent() << " ";
    }
    out << "(" << prefix((*m_iter)->get_name()) << " " <<
        ( (NULL != value) ? render_const_value(type, value) : "nil" ) <<
        " :id " << (*m_iter)->get_key();
    if ( type->is_base_type() && "string" == typespec(type) )
      if ( ((t_base_type*)type)->is_binary() )
        out << " :type binary";
      else
        out << " :type string";
    else
      out << " :type " << typespec(type);
    if ( (*m_iter)->get_req() == t_field::T_OPTIONAL ) {
      out << " :optional t";
    }
    if ( (*m_iter)->has_doc()) {
      out << " :documentation \"" << cl_docstring((*m_iter)->get_doc()) << "\"";
    }
    out <<")";
  }

  out << ")";
}

void t_cl_generator::generate_cl_struct(std::ostream& out, t_struct* tstruct, bool is_exception = false) {
  std::string name = type_name(tstruct);
  out << (is_exception ? "(thrift:def-exception " : "(thrift:def-struct ") <<
      prefix(name) << endl;
  indent_up();
  if ( tstruct->has_doc() ) {
    out << indent() ;
    out << "\"" << cl_docstring(tstruct->get_doc()) << "\"" << endl;
  }
  out << indent() ;
  generate_cl_struct_internal(out, tstruct, is_exception);
  indent_down();
  out << ")" << endl << endl;
}

void t_cl_generator::generate_exception_sig(std::ostream& out, t_function* f) {
  generate_cl_struct_internal(out, f->get_xceptions(), true);
}

void t_cl_generator::generate_service(t_service* tservice) {
  string extends_client;
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;

  if (tservice->get_extends() != NULL) {
    extends_client = type_name(tservice->get_extends());
  }

  extends_client = extends_client.empty() ? "nil" : prefix(extends_client);

  f_types_ << "(thrift:def-service " << prefix(service_name_) << " "
           << extends_client;

  indent_up();

  if ( tservice->has_doc()) {
      f_types_ << endl << indent()
               << "(:documentation \"" << cl_docstring(tservice->get_doc()) << "\")";
    }

  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_function* function = *f_iter;
    string fname = function->get_name();
    string signature = function_signature(function);
    t_struct* exceptions = function->get_xceptions();
    const vector<t_field*>& xmembers = exceptions->get_members();

    f_types_ << endl << indent() << "(:method " << prefix(fname);
    f_types_ << " (" << signature << " "  << typespec((*f_iter)->get_returntype()) << ")";
    if (xmembers.size() > 0) {
      f_types_ << endl << indent() << " :exceptions " ;
      generate_exception_sig(f_types_, function);
    }
    if ( (*f_iter)->is_oneway() ) {
      f_types_ << endl << indent() << " :oneway t";
    }
    if ( (*f_iter)->has_doc() ) {
      f_types_ << endl << indent() << " :documentation \""
               << cl_docstring((*f_iter)->get_doc()) << "\"";
  }
    f_types_ << ")";
  }

  f_types_ << ")" << endl << endl;

  indent_down();
}

string t_cl_generator::typespec(t_type *t) {
  t = get_true_type(t);

  if (t -> is_binary()){
    return "binary";
  } else if (t->is_base_type()) {
    return type_name(t);
  } else if (t->is_map()) {
    t_map *m = (t_map*) t;
    return "(thrift:map " + typespec(m->get_key_type()) + " " +
      typespec(m->get_val_type()) + ")";
  } else if (t->is_struct() || t->is_xception()) {
    return "(struct " + prefix(type_name(t)) + ")";
  } else if (t->is_list()) {
    return "(thrift:list " + typespec(((t_list*) t)->get_elem_type()) + ")";
  } else if (t->is_set()) {
    return "(thrift:set " + typespec(((t_set*) t)->get_elem_type()) + ")";
  } else if (t->is_enum()) {
    return "(enum \"" + ((t_enum*) t)->get_name() + "\")";
  } else {
    throw "Sorry, I don't know how to generate this: " + type_name(t);
  }
}

string t_cl_generator::function_signature(t_function* tfunction) {
  return argument_list(tfunction->get_arglist());
}

string t_cl_generator::argument_list(t_struct* tstruct) {
  stringstream res;
  res << "(";

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      res << " ";
    }
    res << "(" + prefix((*f_iter)->get_name()) << " " <<
      typespec((*f_iter)->get_type()) << " " <<
      (*f_iter)->get_key() <<  ")";


  }
  res << ")";
  return res.str();
}

string t_cl_generator::type_name(t_type* ttype) {
  string prefix = "";
  t_program* program = ttype->get_program();

  if (program != NULL && program != program_)
    prefix = package_of(program) == package() ? "" : package_of(program) + ":";

  string name = ttype->get_name();

  if (ttype->is_struct() || ttype->is_xception())
    name = lowercase(ttype->get_name());

  return prefix + name;
}

THRIFT_REGISTER_GENERATOR(
    cl,
    "Common Lisp",
    "    no_asd:          Do not define ASDF systems for each generated Thrift program.\n"
    "    sys_pref=        The prefix to give ASDF system names. Default: thrift-gen-\n")
