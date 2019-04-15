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
 */

#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <set>

#include <stdlib.h>
#include <sys/stat.h>
#include <sstream>
#include "thrift/platform.h"
#include "thrift/generate/t_oop_generator.h"

using std::map;
using std::ostream;
using std::ostringstream;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

static const string endl = "\n"; // avoid ostream << std::endl flushes

/**
 * Swift 3 code generator.
 *
 * Designed from the Swift/Cocoa code generator(s)
 */
class t_swift_generator : public t_oop_generator {
public:
  t_swift_generator(t_program* program,
                    const map<string, string>& parsed_options,
                    const string& option_string)
    : t_oop_generator(program) {
    (void)option_string;
    map<string, string>::const_iterator iter;

    log_unexpected_ = false;
    async_clients_ = false;
    debug_descriptions_ = false;
    no_strict_ = false;
    namespaced_ = false;
    gen_cocoa_ = false;
    promise_kit_ = false;
    safe_enums_ = false;

    for( iter = parsed_options.begin(); iter != parsed_options.end(); ++iter) {
      if( iter->first.compare("log_unexpected") == 0) {
        log_unexpected_ = true;
      } else if( iter->first.compare("async_clients") == 0) {
        async_clients_ = true;
      } else if( iter->first.compare("no_strict") == 0) {
        no_strict_ = true;
      } else if( iter->first.compare("debug_descriptions") == 0) {
        debug_descriptions_ = true;
      } else if( iter->first.compare("namespaced") == 0) {
        namespaced_ = true;
      } else if( iter->first.compare("cocoa") == 0) {
        gen_cocoa_ = true;
      } else if( iter->first.compare("safe_enums") == 0) {
        safe_enums_ = true;
      } else if( iter->first.compare("promise_kit") == 0) {
        if (gen_cocoa_ == false) {
          throw "PromiseKit only available with Swift 2.x, use `cocoa` option" + iter->first;
        }
        promise_kit_ = true;
      } else {
        throw "unknown option swift:" + iter->first;
      }
    }

    out_dir_base_ = "gen-swift";
  }

  /**
   * Init and close methods
   */

  void init_generator();
  void close_generator();

  void generate_consts(vector<t_const*> consts);

  /**
   * Program-level generation functions
   */

  void generate_typedef(t_typedef* ttypedef);
  void generate_enum(t_enum* tenum);
  void generate_struct(t_struct* tstruct);
  void generate_xception(t_struct* txception);
  void generate_service(t_service* tservice);


  void render_const_value(ostream& out,
                          t_type* type,
                          t_const_value* value);

  void generate_swift_struct(ostream& out,
                             t_struct* tstruct,
                             bool is_private);

  void generate_swift_struct_init(ostream& out,
                                  t_struct* tstruct,
                                  bool all,
                                  bool is_private);

  void generate_swift_struct_implementation(ostream& out,
                                            t_struct* tstruct,
                                            bool is_result,
                                            bool is_private);
  void generate_swift_struct_hashable_extension(ostream& out,
                                                t_struct* tstruct,
                                                bool is_private);
  void generate_swift_struct_equatable_extension(ostream& out,
                                                 t_struct* tstruct,
                                                 bool is_private);
  void generate_swift_struct_thrift_extension(ostream& out,
                                              t_struct* tstruct,
                                              bool is_result,
                                              bool is_private);
  void generate_swift_struct_reader(ostream& out, t_struct* tstruct, bool is_private);


  void generate_swift_struct_printable_extension(ostream& out, t_struct* tstruct);
  void generate_swift_union_reader(ostream& out, t_struct* tstruct);

  string function_result_helper_struct_type(t_service *tservice, t_function* tfunction);
  string function_args_helper_struct_type(t_service* tservice, t_function* tfunction);
  void generate_function_helpers(t_service *tservice, t_function* tfunction);

  /**
   * Service-level generation functions
   */

  void generate_swift_service_protocol(ostream& out, t_service* tservice);
  void generate_swift_service_protocol_async(ostream& out, t_service* tservice);

  void generate_swift_service_client(ostream& out, t_service* tservice);
  void generate_swift_service_client_async(ostream& out, t_service* tservice);

  void generate_swift_service_client_send_function_implementation(ostream& out,
                                                                  t_service* tservice,
                                                                  t_function* tfunction,
                                                                  bool needs_protocol);
  void generate_swift_service_client_send_function_invocation(ostream& out, t_function* tfunction);
  void generate_swift_service_client_send_async_function_invocation(ostream& out,
                                                                    t_function* tfunction);
  void generate_swift_service_client_recv_function_implementation(ostream& out,
                                                                  t_service* tservice,
                                                                  t_function* tfunction,
                                                                  bool needs_protocol);
  void generate_swift_service_client_implementation(ostream& out, t_service* tservice);
  void generate_swift_service_client_async_implementation(ostream& out, t_service* tservice);

  void generate_swift_service_server(ostream& out, t_service* tservice);
  void generate_swift_service_server_implementation(ostream& out, t_service* tservice);
  void generate_swift_service_helpers(t_service* tservice);

  /**
   * Helper rendering functions
   */

  string swift_imports();
  string swift_thrift_imports();
  string type_name(t_type* ttype, bool is_optional=false, bool is_forced=false);
  string base_type_name(t_base_type* tbase);
  string declare_property(t_field* tfield, bool is_private);
  string function_signature(t_function* tfunction);
  string async_function_signature(t_function* tfunction);


  string argument_list(t_struct* tstruct, string protocol_name, bool is_internal);
  string type_to_enum(t_type* ttype, bool qualified=false);
  string maybe_escape_identifier(const string& identifier);
  void populate_reserved_words();
  /** Swift 3 specific */
  string enum_case_name(t_enum_value* tenum_case, bool declaration);
  string enum_const_name(string enum_identifier);
  void function_docstring(ostream& out, t_function* tfunction);
  void async_function_docstring(ostream& out, t_function* tfunction);
  void generate_docstring(ostream& out, string& doc);

  /** Swift 2/Cocoa carryover */
  string promise_function_signature(t_function* tfunction);
  string function_name(t_function* tfunction);
  void generate_old_swift_struct_writer(ostream& out,t_struct* tstruct, bool is_private);
  void generate_old_swift_struct_result_writer(ostream& out, t_struct* tstruct);

  /** Swift 2/Cocoa backwards compatibility*/
  void generate_old_enum(t_enum* tenum);
  void generate_old_swift_struct(ostream& out,
                                 t_struct* tstruct,
                                 bool is_private);
  void generate_old_swift_service_client_async_implementation(ostream& out,
                                                              t_service* tservice);

  static std::string get_real_swift_module(const t_program* program) {
    std::string real_module = program->get_namespace("swift");
    if (real_module.empty()) {
      return program->get_name();
    }
    return real_module;
  }
private:

  void block_open(ostream& out) {
    out << " {" << endl;
    indent_up();
  }

  void block_close(ostream& out, bool end_line=true) {
    indent_down();
    indent(out) << "}";
    if (end_line) out << endl;
  }

  bool field_is_optional(t_field* tfield) {
    bool opt = tfield->get_req() == t_field::T_OPTIONAL;
    if (tfield->annotations_.find("swift.nullable") != tfield->annotations_.end() && tfield->get_req() != t_field::T_REQUIRED) {
      opt = true;
    }
    if (gen_cocoa_) { // Backwards compatibility, only if its actually "optional"
      opt = tfield->get_req() == t_field::T_OPTIONAL;
    }
    return opt;
  }

  bool struct_has_required_fields(t_struct* tstruct) {
    const vector<t_field*>& members = tstruct->get_members();
    vector<t_field*>::const_iterator m_iter;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      if (!field_is_optional(*m_iter)) {
        return true;
      }
    }
    return false;
  }

  bool struct_has_optional_fields(t_struct* tstruct) {
    const vector<t_field*>& members = tstruct->get_members();
    vector<t_field*>::const_iterator m_iter;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      if (field_is_optional(*m_iter)) {
        return true;
      }
    }
    return false;
  }

  string constants_declarations_;

  /**
   * File streams
   */

  ofstream_with_content_based_conditional_update f_decl_;
  ofstream_with_content_based_conditional_update f_impl_;

  bool log_unexpected_;
  bool async_clients_;

  bool debug_descriptions_;
  bool no_strict_;
  bool namespaced_;
  bool safe_enums_;
  set<string> swift_reserved_words_;

  /** Swift 2/Cocoa compatibility */
  bool gen_cocoa_;
  bool promise_kit_;

};

/**
 * Prepares for file generation by opening up the necessary file output
 * streams.
 */
void t_swift_generator::init_generator() {
  // Make output directory
  string module = get_real_swift_module(program_);
  string out_dir = get_out_dir();
  string module_path = out_dir;
  string name = program_name_;
  if (namespaced_ && !module.empty()) {
    module_path = module_path + "/" + module;
    name = module;
  }
  MKDIR(module_path.c_str());

  populate_reserved_words();

  // we have a .swift declarations file...
  string f_decl_name = name + ".swift";
  string f_decl_fullname = module_path + "/" + f_decl_name;
  f_decl_.open(f_decl_fullname.c_str());

  f_decl_ << autogen_comment() << endl;

  f_decl_ << swift_imports() << swift_thrift_imports() << endl;

  // ...and a .swift implementation extensions file
  string f_impl_name = name + "+Exts.swift";
  string f_impl_fullname = module_path + "/" + f_impl_name;
  f_impl_.open(f_impl_fullname.c_str());

  f_impl_ << autogen_comment() << endl;

  f_impl_ << swift_imports() << swift_thrift_imports() << endl;

}

/**
 * Prints standard Cocoa imports
 *
 * @return List of imports for Cocoa libraries
 */
string t_swift_generator::swift_imports() {

  vector<string> includes_list;
  includes_list.push_back("Foundation");

  ostringstream includes;

  vector<string>::const_iterator i_iter;
  for (i_iter=includes_list.begin(); i_iter!=includes_list.end(); ++i_iter) {
    includes << "import " << *i_iter << endl;
  }

  if (namespaced_) {
    const vector<t_program*>& program_includes = program_->get_includes();
    for (size_t i = 0; i < program_includes.size(); ++i) {
      includes << ("import " + get_real_swift_module(program_includes[i])) << endl;
    }
  }
  includes << endl;

  return includes.str();
}

/**
 * Prints Thrift runtime imports
 *
 * @return List of imports necessary for Thrift runtime
 */
string t_swift_generator::swift_thrift_imports() {

  vector<string> includes_list;
  includes_list.push_back("Thrift");

  if (gen_cocoa_ && promise_kit_) {
    includes_list.push_back("PromiseKit");
  }

  ostringstream includes;

  vector<string>::const_iterator i_iter;
  for (i_iter=includes_list.begin(); i_iter!=includes_list.end(); ++i_iter) {
    includes << "import " << *i_iter << endl;
  }

  includes << endl;

  return includes.str();
}

/**
 * Finish up generation.
 */
void t_swift_generator::close_generator() {
  // stick our constants declarations at the end of the header file
  // since they refer to things we are defining.
  f_decl_ << constants_declarations_ << endl;
}

/**
 * Generates a typedef. This is just a simple 1-liner in Swift
 *
 * @param ttypedef The type definition
 */
void t_swift_generator::generate_typedef(t_typedef* ttypedef) {
  f_decl_ << indent() << "public typealias " << ttypedef->get_symbolic()
          << " = " << type_name(ttypedef->get_type()) << endl;
  f_decl_ << endl;
}


/**
 * Generates code for an enumerated type. In Swift, this is
 * essentially the same as the thrift definition itself, using
 * Swift syntax.  Conforms to TEnum which
 * implementes read/write.
 *
 * @param tenum The enumeration
 */
void t_swift_generator::generate_enum(t_enum* tenum) {
  if (gen_cocoa_) {
    generate_old_enum(tenum);
    return;
  }
  f_decl_ << indent() << "public enum " << tenum->get_name() << " : TEnum";
  block_open(f_decl_);

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;

  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    f_decl_ << indent() << "case " << enum_case_name((*c_iter), true) << endl;
  }

  // unknown associated value case for safety and similar behavior to other languages
  if (safe_enums_) {
    f_decl_ << indent() << "case unknown(Int32)" << endl;
  }
  f_decl_ << endl;

  // TSerializable read(from:)
  f_decl_ << indent() << "public static func read(from proto: TProtocol) throws -> "
          << tenum->get_name();
  block_open(f_decl_);
  f_decl_ << indent() << "let raw: Int32 = try proto.read()" << endl;
  f_decl_ << indent() << "let new = " << tenum->get_name() << "(rawValue: raw)" << endl;

  f_decl_ << indent() << "if let unwrapped = new {" << endl;
  indent_up();
  f_decl_ << indent() << "return unwrapped" << endl;
  indent_down();
  f_decl_ << indent() << "} else {" << endl;
  indent_up();
  f_decl_ << indent() << "throw TProtocolError(error: .invalidData," << endl;
  f_decl_ << indent() << "                     message: \"Invalid enum value (\\(raw)) for \\("
          << tenum->get_name() << ".self)\")" << endl;
  indent_down();
  f_decl_ << indent() << "}" << endl;
  block_close(f_decl_);

  // empty init for TSerializable
  f_decl_ << endl;
  f_decl_ << indent() << "public init()";
  block_open(f_decl_);

  f_decl_ << indent() << "self = ." << enum_case_name(constants.front(), false) << endl;
  block_close(f_decl_);
  f_decl_ << endl;

  // rawValue getter
  f_decl_ << indent() << "public var rawValue: Int32";
  block_open(f_decl_);
  f_decl_ << indent() << "switch self {" << endl;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    f_decl_ << indent() << "case ." << enum_case_name((*c_iter), true)
            << ": return " << (*c_iter)->get_value() << endl;
  }
  if (safe_enums_) {
    f_decl_ << indent() << "case .unknown(let value): return value" << endl;
  }
  f_decl_ << indent() << "}" << endl;
  block_close(f_decl_);
  f_decl_ << endl;

  // convenience rawValue initalizer
  f_decl_ << indent() << "public init?(rawValue: Int32)";
  block_open(f_decl_);
  f_decl_ << indent() << "switch rawValue {" << endl;;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    f_decl_ << indent() << "case " << (*c_iter)->get_value()
            << ": self = ." << enum_case_name((*c_iter), true) << endl;
  }
  if (!safe_enums_) {
    f_decl_ << indent() << "default: return nil" << endl;
  } else {
    f_decl_ << indent() << "default: self = .unknown(rawValue)" << endl;
  }
  f_decl_ << indent() << "}" << endl;
  block_close(f_decl_);




  block_close(f_decl_);
  f_decl_ << endl;
}

/**
 * Generates code for an enumerated type. This is for Swift 2.x/Cocoa
 * backwards compatibility
 *
 * @param tenum The enumeration
 */
void t_swift_generator::generate_old_enum(t_enum* tenum) {
  f_decl_ << indent() << "public enum " << tenum->get_name() << " : Int32";
  block_open(f_decl_);

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;

  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    f_decl_ << indent() << "case " << (*c_iter)->get_name()
            << " = " << (*c_iter)->get_value() << endl;
  }

  f_decl_ << endl;
  f_decl_ << indent() << "public init() { self.init(rawValue: " << constants.front()->get_value() << ")! }" << endl;

  block_close(f_decl_);
  f_decl_ << endl;

  f_impl_ << indent() << "extension " << tenum->get_name() << " : TEnum";
  block_open(f_impl_);

  f_impl_ << endl;

  f_impl_ << indent() << "public static func readValueFromProtocol(proto: TProtocol) throws -> " << tenum->get_name();
  block_open(f_impl_);
  f_impl_ << indent() << "var raw = Int32()" << endl
          << indent() << "try proto.readI32(&raw)" << endl
          << indent() << "return " << tenum->get_name() << "(rawValue: raw)!" << endl;
  block_close(f_impl_);
  f_impl_ << endl;

  f_impl_ << indent() << "public static func writeValue(value: " << tenum->get_name() << ", toProtocol proto: TProtocol) throws";
  block_open(f_impl_);
  f_impl_ << indent() << "try proto.writeI32(value.rawValue)" << endl;
  block_close(f_impl_);
  f_impl_ << endl;

  block_close(f_impl_);
  f_impl_ << endl;
}

string t_swift_generator::enum_case_name(t_enum_value* tenum_case, bool declaration) {
  string name = tenum_case->get_name();
  // force to lowercase for Swift style, maybe escape if its a keyword
  std::transform(name.begin(), name.end(), name.begin(), ::tolower);
  if (declaration) {
    name = maybe_escape_identifier(name);
  }
  return name;
}

/**
 * Renders a constant enum value by transforming the value portion to lowercase
 * for Swift style.
 */
string t_swift_generator::enum_const_name(string enum_identifier) {
  string::iterator it;
  for (it = enum_identifier.begin(); it < enum_identifier.end(); ++it) {
    if ((*it) == '.') {
      break;
    }
  }
  std::transform(it, enum_identifier.end(), it, ::tolower);
  return enum_identifier;
}

/**
 * Generates public constants for all Thrift constants.
 *
 * @param consts Constants to generate
 */
void t_swift_generator::generate_consts(vector<t_const*> consts) {

  ostringstream const_interface;

  // Public constants for base types & strings
  vector<t_const*>::iterator c_iter;
  for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter) {
    t_type* type = (*c_iter)->get_type();
    const_interface << "public let " << capitalize((*c_iter)->get_name()) << " : " << type_name(type) << " = ";
    render_const_value(const_interface, type, (*c_iter)->get_value());
    const_interface << endl << endl;
  }

  // this gets spit into the header file in ::close_generator
  constants_declarations_ = const_interface.str();

}

/**
 * Generates a struct definition for a thrift data type. This is a struct
 * with public members. Optional types are used for optional properties to
 * allow them to be tested for availability. Separate inits are included for
 * required properties & all properties.
 *
 * Generates extensions to provide conformance to TStruct, TSerializable,
 * Hashable & Equatable
 *
 * @param tstruct The struct definition
 */
void t_swift_generator::generate_struct(t_struct* tstruct) {
  generate_swift_struct(f_decl_, tstruct, false);
  generate_swift_struct_implementation(f_impl_, tstruct, false, false);
}

/**
 * Exceptions are structs, but they conform to Error
 *
 * @param tstruct The struct definition
 */
void t_swift_generator::generate_xception(t_struct* txception) {
  generate_swift_struct(f_decl_, txception, false);
  generate_swift_struct_implementation(f_impl_, txception, false, false);
}

void t_swift_generator::generate_docstring(ostream& out, string& doc) {
  if (doc != "") {
    std::vector<std::string> strings;

    std::string::size_type pos = 0;
    std::string::size_type prev = 0;
    while (((pos = doc.find("\n", prev)) != std::string::npos)
        || ((pos = doc.find("\r", prev)) != std::string::npos)
        || ((pos = doc.find("\r\n", prev)) != std::string::npos))
    {
        strings.push_back(doc.substr(prev, pos - prev));
        prev = pos + 1;
    }

    // To get the last substring (or only, if delimiter is not found)
    strings.push_back(doc.substr(prev));

    vector<string>::const_iterator d_iter;
    for (d_iter = strings.begin(); d_iter != strings.end(); ++d_iter) {
      if ((*d_iter) != "") {
        out << indent() << "/// " << (*d_iter) << endl;
      }
    }
  }
}



/**
 * Generate the interface for a struct. Only properties and
 * init methods are included.
 *
 * @param tstruct The struct definition
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_swift_struct(ostream& out,
                                              t_struct* tstruct,
                                              bool is_private) {

  if (gen_cocoa_) {
    generate_old_swift_struct(out, tstruct, is_private);
    return;
  }
  string doc = tstruct->get_doc();
  generate_docstring(out, doc);


  // properties
  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;


  if (tstruct->is_union()) {
    // special, unions
    out << indent() << "public enum " << tstruct->get_name();
    block_open(out);
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      out << endl;
      string doc = (*m_iter)->get_doc();
      generate_docstring(out, doc);
      out << indent() << "case "
          << maybe_escape_identifier((*m_iter)->get_name()) << "(val: "
          << type_name((*m_iter)->get_type(), false) << ")" << endl;
    }
  } else {
    // Normal structs

    string visibility = is_private ? (gen_cocoa_ ? "private" : "fileprivate") : "public";

    out << indent() << visibility << " final class " << tstruct->get_name();

    if (tstruct->is_xception()) {
      out << " : Swift.Error"; // Error seems to be a common exception name in thrift
    }

    block_open(out);
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      out << endl;
      // TODO: Defaults

      string doc = (*m_iter)->get_doc();
      generate_docstring(out, doc);

      out << indent() << declare_property(*m_iter, is_private) << endl;
    }

    out << endl;
    out << endl;

    if (!struct_has_required_fields(tstruct)) {
      indent(out) << visibility << " init() { }" << endl;
    }
    if (struct_has_required_fields(tstruct)) {
      generate_swift_struct_init(out, tstruct, false, is_private);
    }
    if (struct_has_optional_fields(tstruct)) {
      generate_swift_struct_init(out, tstruct, true, is_private);
    }
  }

  block_close(out);

  out << endl;
}

/**
 * Legacy Swift2/Cocoa generator
 *
 * @param tstruct
 * @param is_private
 */


void t_swift_generator::generate_old_swift_struct(ostream& out,
                                                  t_struct* tstruct,
                                                  bool is_private) {
  string visibility = is_private ? "private" : "public";

  out << indent() << visibility << " final class " << tstruct->get_name();

  if (tstruct->is_xception()) {
    out << " : ErrorType";
  }

  block_open(out);

  // properties
  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    out << endl;
    out << indent() << declare_property(*m_iter, is_private) << endl;
  }

  out << endl;

  // init

  indent(out) << visibility << " init()";
  block_open(out);
  block_close(out);

  out << endl;

  if (struct_has_required_fields(tstruct)) {
    generate_swift_struct_init(out, tstruct, false, is_private);
  }
  if (struct_has_optional_fields(tstruct)) {
    generate_swift_struct_init(out, tstruct, true, is_private);
  }

  block_close(out);

  out << endl;
}

/**
 * Generate struct init for properties
 *
 * @param tstruct The structure definition
 * @param all     Generate init with all or just required properties
 * @param is_private
 *                Is the initializer public or private
 */
void t_swift_generator::generate_swift_struct_init(ostream& out,
                                                   t_struct* tstruct,
                                                   bool all,
                                                   bool is_private) {

  string visibility = is_private ? (gen_cocoa_ ? "private" : "fileprivate") : "public";

  indent(out) << visibility << " init(";

  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  bool first=true;
  for (m_iter = members.begin(); m_iter != members.end();) {
    if (all || !field_is_optional(*m_iter)) {
      if (first) {
        first = false;
      }
      else {
        out << ", ";
      }
      out << (*m_iter)->get_name() << ": "
          << maybe_escape_identifier(type_name((*m_iter)->get_type(), field_is_optional(*m_iter)));
    }
    ++m_iter;
  }
  out << ")";

  block_open(out);

  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    if (!gen_cocoa_) {
      bool should_set = all;
      should_set = should_set || !field_is_optional((*m_iter));
      if (should_set) {
        out << indent() << "self." << maybe_escape_identifier((*m_iter)->get_name()) << " = "
            << maybe_escape_identifier((*m_iter)->get_name()) << endl;
      }
    } else {
      /** legacy Swift2/Cocoa */
      if (all || (*m_iter)->get_req() == t_field::T_REQUIRED || (*m_iter)->get_req() == t_field::T_OPT_IN_REQ_OUT) {
        out << indent() << "self." << maybe_escape_identifier((*m_iter)->get_name()) << " = "
            << maybe_escape_identifier((*m_iter)->get_name()) << endl;
      }
    }
  }

  block_close(out);

  out << endl;
}

/**
 * Generate the hashable protocol implmentation
 *
 * @param tstruct The structure definition
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_swift_struct_hashable_extension(ostream& out,
                                                                 t_struct* tstruct,
                                                                 bool is_private) {

  string visibility = is_private ? (gen_cocoa_ ? "private" : "fileprivate") : "public";
  indent(out) << "extension " << tstruct->get_name() << " : Hashable";
  block_open(out);
  out << endl;
  indent(out) << visibility << " var hashValue : Int";
  block_open(out);

  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  if (!members.empty()) {
    indent(out) << "let prime = 31" << endl;
    indent(out) << "var result = 1" << endl;
    if (!tstruct->is_union()) {
      for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
        t_field* tfield = *m_iter;
        string accessor = field_is_optional(tfield) ? "?." : ".";
        string defaultor = field_is_optional(tfield) ? " ?? 0" : "";
        indent(out) << "result = prime &* result &+ (" << maybe_escape_identifier(tfield->get_name()) << accessor
                    <<  "hashValue" << defaultor << ")" << endl;
      }
    } else {
      indent(out) << "switch self {" << endl;
      for (m_iter = members.begin(); m_iter != members.end(); m_iter++) {
        t_field *tfield = *m_iter;
        indent(out) << "case ." << tfield->get_name() << "(let val): result = prime &* val.hashValue" << endl;
      }
      indent(out) << "}" << endl << endl;
    }
    indent(out) << "return result" << endl;
  }
  else {
    indent(out) << "return 31" << endl;
  }

  block_close(out);
  out << endl;
  block_close(out);
  out << endl;
}

/**
 * Generate the equatable protocol implementation
 *
 * @param tstruct The structure definition
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_swift_struct_equatable_extension(ostream& out,
                                                                  t_struct* tstruct,
                                                                  bool is_private) {

  string visibility = is_private ? (gen_cocoa_ ? "private" : "fileprivate") : "public";

  indent(out) << visibility << " func ==(lhs: " << type_name(tstruct) << ", rhs: "
              << type_name(tstruct) << ") -> Bool";
  block_open(out);
  indent(out) << "return";

  const vector<t_field*>& members = tstruct->get_members();
  vector<t_field*>::const_iterator m_iter;

  if (members.size()) {
    if (!tstruct->is_union()) {
      out << endl;
      indent_up();

      for (m_iter = members.begin(); m_iter != members.end();) {
        t_field* tfield = *m_iter;
        indent(out) << "(lhs." << maybe_escape_identifier(tfield->get_name())
                    << (gen_cocoa_ ? " ?" : " ") << "== rhs." << maybe_escape_identifier(tfield->get_name()) << ")"; // swift 2 ?== operator not in 3?
        if (++m_iter != members.end()) {
          out << " &&";
        }
        out << endl;
      }
      indent_down();
    } else {
      block_open(out);
      indent(out) << "switch (lhs, rhs) {" << endl;
      for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
        t_field* tfield = *m_iter;
        indent(out) << "case (." << tfield->get_name() << "(let lval), ."
                    << tfield->get_name() << "(let rval)): return lval == rval"
                    << endl;
      }
      indent(out) << "default: return false" << endl;
      indent(out) << "}" << endl;
      indent_down();
      indent(out) << "}()" << endl;
    }
  }
  else {
    out << " true" << endl;
  }

  block_close(out);
  out << endl;
}

/**
 * Generate struct implementation. Produces extensions that
 * fulfill the requisite protocols to complete the value.
 *
 * @param tstruct The struct definition
 * @param is_result
 *                If this is a result it needs a different writer
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_swift_struct_implementation(ostream& out,
                                                             t_struct* tstruct,
                                                             bool is_result,
                                                             bool is_private) {

  generate_swift_struct_equatable_extension(out, tstruct, is_private);

  if (!is_private && !is_result && !gen_cocoa_) {  // old compiler didn't use debug_descriptions, OR it with gen_cocoa_ so the flag doesn't matter w/ cocoa
    generate_swift_struct_printable_extension(out, tstruct);
  }

  generate_swift_struct_hashable_extension(out, tstruct, is_private);
  generate_swift_struct_thrift_extension(out, tstruct, is_result, is_private);

  out << endl << endl;
}

/**
 * Generate the TStruct protocol implementation.
 *
 * @param tstruct The structure definition
 * @param is_result
 *                Is the struct a result value
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_swift_struct_thrift_extension(ostream& out,
                                                               t_struct* tstruct,
                                                               bool is_result,
                                                               bool is_private) {

  indent(out) << "extension " << tstruct->get_name() << " : TStruct";

  block_open(out);

  out << endl;
  if (!gen_cocoa_) {
    /** Swift 3, no writer we just write field ID's */
    string access = (is_private) ? (gen_cocoa_ ? "private" : "fileprivate") : "public";
    // generate fieldID's dictionary
    out << indent() << access << " static var fieldIds: [String: Int32]";
    block_open(out);
    out << indent() << "return [";
    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;
    bool wrote = false;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      wrote = true;
      out << "\"" << (*f_iter)->get_name() << "\": " << (*f_iter)->get_key() << ", ";
    }
    if (!wrote) {
      // pad a colon
      out << ":";
    }
    out << "]" << endl;
    block_close(out);
    out << endl;
    out << indent() << access << " static var structName: String { return \""
        << tstruct->get_name() << "\" }" << endl << endl;

    if (tstruct->is_union()) {
      generate_swift_union_reader(out, tstruct);
    } else {
      generate_swift_struct_reader(out, tstruct, is_private);
    }
  } else {
    /** Legacy Swift2/Cocoa */

    generate_swift_struct_reader(out, tstruct, is_private);

    if (is_result) {
      generate_old_swift_struct_result_writer(out, tstruct);
    }
    else {
      generate_old_swift_struct_writer(out, tstruct, is_private);
    }
  }

  block_close(out);
  out << endl;
}

void t_swift_generator::generate_swift_union_reader(ostream& out, t_struct* tstruct) {
  indent(out) << "public static func read(from proto: TProtocol) throws -> "
              << tstruct->get_name();
  block_open(out);
  indent(out) << "_ = try proto.readStructBegin()" << endl;

  indent(out) << "var ret: " << tstruct->get_name() << "?";
  out << endl;
  indent(out) << "fields: while true";
  block_open(out);
  out << endl;
  indent(out) << "let (_, fieldType, fieldID) = try proto.readFieldBegin()" << endl << endl;
  indent(out) << "switch (fieldID, fieldType)";
  block_open(out);
  indent(out) << "case (_, .stop):            break fields" << endl;

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    indent(out) << "case (" << (*f_iter)->get_key() << ", " << type_to_enum((*f_iter)->get_type()) << "):";
    string padding = "";

    t_type* type = get_true_type((*f_iter)->get_type());
    if (type->is_base_type()) {
      t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
      switch (tbase) {
        case t_base_type::TYPE_STRING:
        case t_base_type::TYPE_DOUBLE:
          padding = "           ";
          break;

        case t_base_type::TYPE_BOOL:
        case t_base_type::TYPE_I8:
          padding = "            ";
          break;
        case t_base_type::TYPE_I16:
        case t_base_type::TYPE_I32:
        case t_base_type::TYPE_I64:
          padding = "             ";
          break;
        default: break;
      }
    } else if (type->is_enum() || type->is_set() || type->is_map()) {
      padding = "             ";
    } else if (type->is_struct() || type->is_xception()) {
      padding = "           ";
    } else if (type->is_list()) {
      padding = "            ";
    }

    indent(out) << padding << "ret = " << tstruct->get_name() << "."
                << (*f_iter)->get_name() << "(val: " << "try "
                << type_name((*f_iter)->get_type(), false, false)
                << ".read(from: proto))" << endl;
  }

  indent(out) << "case let (_, unknownType):  try proto.skip(type: unknownType)" << endl;

  block_close(out);
  indent(out) << "try proto.readFieldEnd()" << endl;

  block_close(out);
  out << endl;

  indent(out) << "try proto.readStructEnd()" << endl;

  indent(out) << "if let ret = ret";
  block_open(out);
  indent(out) << "return ret" << endl;
  block_close(out);
  out << endl;
  indent(out) << "throw TProtocolError(error: .unknown, message: \"Missing required value for type: "
              << tstruct->get_name() << "\")";
  block_close(out);
  out << endl;

}

/**
 * Generates a function to read a struct from
 * from a protocol. (TStruct compliance)
 *
 * @param tstruct The structure definition
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_swift_struct_reader(ostream& out,
                                                     t_struct* tstruct,
                                                     bool is_private) {

  if (!gen_cocoa_) {
    /** Swift 3 case */
    string visibility = is_private ? "fileprivate" : "public";

    indent(out) << visibility << " static func read(from proto: TProtocol) throws -> "
               << tstruct->get_name();

    block_open(out);
    indent(out) << "_ = try proto.readStructBegin()" << endl;

    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      bool optional = field_is_optional(*f_iter);
      indent(out) << "var " << maybe_escape_identifier((*f_iter)->get_name()) << ": "
                  << type_name((*f_iter)->get_type(), optional, !optional) << endl;
    }

    out << endl;

    // Loop over reading in fields
    indent(out) << "fields: while true";
    block_open(out);
    out << endl;

    indent(out) << "let (_, fieldType, fieldID) = try proto.readFieldBegin()" << endl << endl;
    indent(out) << "switch (fieldID, fieldType)";
    block_open(out);
    indent(out) << "case (_, .stop):            break fields" << endl;


    // Generate deserialization code for known cases
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      indent(out) << "case (" << (*f_iter)->get_key() << ", " << type_to_enum((*f_iter)->get_type()) << "):";
      string padding = "";

      t_type* type = get_true_type((*f_iter)->get_type());
      if (type->is_base_type()) {
        t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
        switch (tbase) {
          case t_base_type::TYPE_STRING:
          case t_base_type::TYPE_DOUBLE:
            padding = "           ";
          break;

          case t_base_type::TYPE_BOOL:
          case t_base_type::TYPE_I8:
            padding = "            ";
          break;
          case t_base_type::TYPE_I16:
          case t_base_type::TYPE_I32:
          case t_base_type::TYPE_I64:
            padding = "             ";
          break;
          default: break;
        }
      } else if (type->is_enum() || type->is_set() || type->is_map()) {
        padding = "             ";
      } else if (type->is_struct() || type->is_xception()) {
        padding = "           ";
      } else if (type->is_list()) {
        padding = "            ";
      }

      out << padding << maybe_escape_identifier((*f_iter)->get_name()) << " = try "
          << type_name((*f_iter)->get_type(), false, false) << ".read(from: proto)" << endl;
    }

    indent(out) << "case let (_, unknownType):  try proto.skip(type: unknownType)" << endl;
    block_close(out);
    out << endl;

    // Read field end marker
    indent(out) << "try proto.readFieldEnd()" << endl;
    block_close(out);
    out << endl;
    indent(out) << "try proto.readStructEnd()" << endl;

    if (struct_has_required_fields(tstruct)) {
      // performs various checks (e.g. check that all required fields are set)
      indent(out) << "// Required fields" << endl;

      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if (field_is_optional(*f_iter)) {
          continue;
        }
        indent(out) << "try proto.validateValue(" << (*f_iter)->get_name() << ", "
                    << "named: \"" << (*f_iter)->get_name() << "\")" << endl;
      }
    }

    out << endl;

    indent(out) << "return " << tstruct->get_name() << "(";
    for (f_iter = fields.begin(); f_iter != fields.end();) {
      out << (*f_iter)->get_name() << ": " << maybe_escape_identifier((*f_iter)->get_name());
      if (++f_iter != fields.end()) {
        out << ", ";
      }
    }

  } else {
    /** Legacy Swif2/Cocoa case */
    string visibility = is_private ? "private" : "public";

    indent(out) << visibility << " static func readValueFromProtocol(__proto: TProtocol) throws -> "
                << tstruct->get_name();

    block_open(out);
    out << endl;
    indent(out) << "try __proto.readStructBegin()" << endl << endl;

    const vector<t_field*>& fields = tstruct->get_members();
    vector<t_field*>::const_iterator f_iter;

    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      bool optional = field_is_optional(*f_iter);
      indent(out) << "var " << maybe_escape_identifier((*f_iter)->get_name()) << " : "
                  << type_name((*f_iter)->get_type(), optional, !optional) << endl;
    }

    out << endl;

    // Loop over reading in fields
    indent(out) << "fields: while true";
    block_open(out);
    out << endl;

    indent(out) << "let (_, fieldType, fieldID) = try __proto.readFieldBegin()" << endl << endl;
    indent(out) << "switch (fieldID, fieldType)";

    block_open(out);

    indent(out) << "case (_, .STOP):" << endl;
    indent_up();
    indent(out) << "break fields" << endl << endl;
    indent_down();

    // Generate deserialization code for known cases
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {

      indent(out) << "case (" << (*f_iter)->get_key() << ", " << type_to_enum((*f_iter)->get_type()) << "):" << endl;
      indent_up();
      indent(out) << maybe_escape_identifier((*f_iter)->get_name()) << " = try __proto.readValue() as "
                  << type_name((*f_iter)->get_type()) << endl << endl;
      indent_down();

    }

    indent(out) << "case let (_, unknownType):" << endl;
    indent_up();
    indent(out) << "try __proto.skipType(unknownType)" << endl;
    indent_down();
    block_close(out);
    out << endl;

    // Read field end marker
    indent(out) << "try __proto.readFieldEnd()" << endl;

    block_close(out);
    out << endl;
    indent(out) << "try __proto.readStructEnd()" << endl;
    out << endl;

    if (struct_has_required_fields(tstruct)) {
      // performs various checks (e.g. check that all required fields are set)
      indent(out) << "// Required fields" << endl;

      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if (field_is_optional(*f_iter)) {
          continue;
        }
        indent(out) << "try __proto.validateValue(" << (*f_iter)->get_name() << ", "
                    << "named: \"" << (*f_iter)->get_name() << "\")" << endl;
      }
    }

    out << endl;

    indent(out) << "return " << tstruct->get_name() << "(";
    for (f_iter = fields.begin(); f_iter != fields.end();) {
      out << (*f_iter)->get_name() << ": " << maybe_escape_identifier((*f_iter)->get_name());
      if (++f_iter != fields.end()) {
        out << ", ";
      }
    }
  }
  out << ")" << endl;

  block_close(out);

  out << endl;
}

/**
 * Generates a function to write a struct to
 * a protocol. (TStruct compliance) ONLY FOR SWIFT2/COCOA
 *
 * @param tstruct The structure definition
 * @param is_private
 *                Is the struct public or private
 */
void t_swift_generator::generate_old_swift_struct_writer(ostream& out,
                                                         t_struct* tstruct,
                                                         bool is_private) {

  string visibility = is_private ? "private" : "public";

  indent(out) << visibility << " static func writeValue(__value: " << tstruct->get_name()
              << ", toProtocol __proto: TProtocol) throws";
  block_open(out);
  out << endl;

  string name = tstruct->get_name();
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  indent(out) << "try __proto.writeStructBeginWithName(\"" << name << "\")" << endl;
  out << endl;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field *tfield = *f_iter;

    bool optional = field_is_optional(tfield);
    if (optional) {
      indent(out) << "if let " << maybe_escape_identifier(tfield->get_name())
                  << " = __value." << maybe_escape_identifier(tfield->get_name());
      block_open(out);
    }

    indent(out) << "try __proto.writeFieldValue("
                << (optional ? "" : "__value.") << maybe_escape_identifier(tfield->get_name()) << ", "
                << "name: \"" << tfield->get_name() << "\", "
                << "type: " << type_to_enum(tfield->get_type()) << ", "
                << "id: " << tfield->get_key() << ")" << endl;

    if (optional) {
      block_close(out);
    }

    out << endl;
  }

  indent(out) << "try __proto.writeFieldStop()" << endl << endl;
  indent(out) << "try __proto.writeStructEnd()" << endl;
  block_close(out);
  out << endl;
}

/**
 * Generates a function to read a struct from
 * from a protocol. (TStruct compliance)  ONLY FOR SWIFT 2/COCOA
 *
 * This is specifically a function result. Only
 * the first available field is written.
 *
 * @param tstruct The structure definition
 */
void t_swift_generator::generate_old_swift_struct_result_writer(ostream& out, t_struct* tstruct) {

  indent(out) << "private static func writeValue(__value: " << tstruct->get_name()
              << ", toProtocol __proto: TProtocol) throws";
  block_open(out);
  out << endl;
  string name = tstruct->get_name();
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;
  indent(out) << "try __proto.writeStructBeginWithName(\"" << name << "\")" << endl;
  out << endl;

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field *tfield = *f_iter;

    indent(out) << "if let result = __value." << (*f_iter)->get_name();

    block_open(out);

    indent(out) << "try __proto.writeFieldValue(result, "
                << "name: \"" << tfield->get_name() << "\", "
                << "type: " << type_to_enum(tfield->get_type()) << ", "
                << "id: " << tfield->get_key() << ")" << endl;

    block_close(out);
  }
  // Write the struct map
  indent(out) << "try __proto.writeFieldStop()" << endl << endl;
  indent(out) << "try __proto.writeStructEnd()" << endl;
  block_close(out);
  out << endl;
}

/**
 * Generates a description method for the given struct
 *
 * @param tstruct The struct definition
 */
void t_swift_generator::generate_swift_struct_printable_extension(ostream& out, t_struct* tstruct) {

  // Allow use of debugDescription so the app can add description via a cateogory/extension

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  indent(out) << "extension " << tstruct->get_name() << " : "
              << (debug_descriptions_ ? "CustomDebugStringConvertible" : "CustomStringConvertible");

  block_open(out);
  out << endl;
  indent(out) << "public var description : String";
  block_open(out);
  indent(out) << "var desc = \"" << tstruct->get_name();

  if (!gen_cocoa_) {
    if (!tstruct->is_union()) {
      out << "(\"" << endl;
      for (f_iter = fields.begin(); f_iter != fields.end();) {
        indent(out) << "desc += \"" << (*f_iter)->get_name()
                    << "=\\(String(describing: self." << maybe_escape_identifier((*f_iter)->get_name()) << "))";
        if (++f_iter != fields.end()) {
          out << ", ";
        }
        out << "\"" << endl;
      }
    } else {
      out << ".\"" << endl;
      indent(out) << "switch self {" << endl;
      for (f_iter = fields.begin(); f_iter != fields.end();f_iter++) {
        indent(out) << "case ." << (*f_iter)->get_name() << "(let val): "
                    << "desc += \"" << (*f_iter)->get_name() << "(val: \\(val))\""
                    << endl;
      }
      indent(out) << "}" << endl;
    }
  } else {
    out << "(\"" << endl;
    for (f_iter = fields.begin(); f_iter != fields.end();) {
      indent(out) << "desc += \"" << (*f_iter)->get_name()
                  << "=\\(self." << maybe_escape_identifier((*f_iter)->get_name()) << ")";
      if (++f_iter != fields.end()) {
        out << ", ";
      }
      out << "\"" << endl;
    }
    indent(out) << "desc += \")\"" << endl;
  }

  indent(out) << "return desc" << endl;
  block_close(out);
  out << endl;
  block_close(out);
  out << endl;
}

/**
 * Generates a thrift service.  In Swift this consists of a
 * protocol definition and a client (with it's implementation
 * separated into exts file).
 *
 * @param tservice The service definition
 */
void t_swift_generator::generate_service(t_service* tservice) {

  generate_swift_service_protocol(f_decl_, tservice);
  generate_swift_service_client(f_decl_, tservice);
  if (async_clients_) {
    generate_swift_service_protocol_async(f_decl_, tservice);
    generate_swift_service_client_async(f_decl_, tservice);
  }
  generate_swift_service_server(f_decl_, tservice);

  generate_swift_service_helpers(tservice);

  generate_swift_service_client_implementation(f_impl_, tservice);
  if (async_clients_) {
    generate_swift_service_client_async_implementation(f_impl_, tservice);
  }
  generate_swift_service_server_implementation(f_impl_, tservice);
}

/**
 * Generates structs for all the service return types
 *
 * @param tservice The service
 */
void t_swift_generator::generate_swift_service_helpers(t_service* tservice) {
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {

    t_struct* ts = (*f_iter)->get_arglist();

    string qname = function_args_helper_struct_type(tservice, *f_iter);

    t_struct qname_ts = t_struct(ts->get_program(), qname);

    const vector<t_field*>& members = ts->get_members();
    vector<t_field*>::const_iterator m_iter;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      qname_ts.append(*m_iter);
    }

    generate_swift_struct(f_impl_, &qname_ts, true);
    generate_swift_struct_implementation(f_impl_, &qname_ts, false, true);
    generate_function_helpers(tservice, *f_iter);
  }
}

string t_swift_generator::function_result_helper_struct_type(t_service *tservice, t_function* tfunction) {
  if (tfunction->is_oneway()) {
    return tservice->get_name() + "_" + tfunction->get_name();
  } else {
    return tservice->get_name() + "_" + tfunction->get_name() + "_result";
  }
}

string t_swift_generator::function_args_helper_struct_type(t_service *tservice, t_function* tfunction) {
  return tservice->get_name() + "_" + tfunction->get_name() + "_args";
}

/**
 * Generates a struct and helpers for a function.
 *
 * @param tfunction The function
 */
void t_swift_generator::generate_function_helpers(t_service *tservice, t_function* tfunction) {
  if (tfunction->is_oneway()) {
    return;
  }

  // create a result struct with a success field of the return type,
  // and a field for each type of exception thrown
  t_struct result(program_, function_result_helper_struct_type(tservice, tfunction));
  if (!tfunction->get_returntype()->is_void()) {
    t_field* success = new t_field(tfunction->get_returntype(), "success", 0);
    success->set_req(t_field::T_OPTIONAL);
    result.append(success);
  }

  t_struct* xs = tfunction->get_xceptions();
  const vector<t_field*>& fields = xs->get_members();
  vector<t_field*>::const_iterator f_iter;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    t_field *x = *f_iter;
    t_field *ox = new t_field(x->get_type(), x->get_name(), x->get_key());
    ox->set_req(t_field::T_OPTIONAL);
    result.append(ox);
  }

  // generate the result struct
  generate_swift_struct(f_impl_, &result, true);
  generate_swift_struct_implementation(f_impl_, &result, true, true);

  for (f_iter = result.get_members().begin(); f_iter != result.get_members().end(); ++f_iter) {
    delete *f_iter;
  }
}

/**
 * Generates a service protocol definition.
 *
 * @param tservice The service to generate a protocol definition for
 */
void t_swift_generator::generate_swift_service_protocol(ostream& out, t_service* tservice) {
  if (!gen_cocoa_) {
    string doc = tservice->get_doc();
    generate_docstring(out, doc);

    indent(out) << "public protocol " << tservice->get_name();
    t_service* parent = tservice->get_extends();
    if (parent != NULL) {
      out << " : " << parent->get_name();
    }
    block_open(out);
    out << endl;

    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::iterator f_iter;

    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      function_docstring(out, *f_iter);
      indent(out) << function_signature(*f_iter) << endl << endl;
    }

  } else {
    indent(out) << "public protocol " << tservice->get_name();
    block_open(out);

    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::iterator f_iter;

    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      out << endl;
      indent(out) << function_signature(*f_iter) << "  // exceptions: ";
      t_struct* xs = (*f_iter)->get_xceptions();
      const vector<t_field*>& xceptions = xs->get_members();
      vector<t_field*>::const_iterator x_iter;
      for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
        out << type_name((*x_iter)->get_type()) + ", ";
      }
      out << endl;
    }
  }
  block_close(out);
  out << endl;
}

/**
 * Generates an asynchronous service protocol definition.
 *
 * @param tservice The service to generate a protocol definition for
 */
void t_swift_generator::generate_swift_service_protocol_async(ostream& out, t_service* tservice) {
  if (!gen_cocoa_) {
    string doc = tservice->get_doc();
    generate_docstring(out, doc);
  }
  indent(out) << "public protocol " << tservice->get_name() << "Async";

  block_open(out);
  if (!gen_cocoa_) {  out << endl; }

  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;

  if (!gen_cocoa_) {
    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      async_function_docstring(out, *f_iter);
      indent(out) << async_function_signature(*f_iter) << endl << endl;
    }
  } else {
    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      out << endl;
      indent(out) << async_function_signature(*f_iter) << endl;
      if (promise_kit_) {
        indent(out) << promise_function_signature(*f_iter) << endl;
      } //
      out << endl;
    }
  }
  block_close(out);
  out << endl;
}

/**
 * Generates a service client interface definition.
 *
 * @param tservice The service to generate a client interface definition for
 */
void t_swift_generator::generate_swift_service_client(ostream& out, t_service* tservice) {
  if (!gen_cocoa_) {
    indent(out) << "open class " << tservice->get_name() << "Client";// : "

    // Inherit from ParentClient
    t_service* parent = tservice->get_extends();
    out << " : " << ((parent == NULL) ? "TClient" : parent->get_name() + "Client");
    out <<  " /* , " << tservice->get_name() << " */";
    block_open(out);
    out << endl;
  } else {
    // a
    indent(out) << "public class " << tservice->get_name() << "Client /* : " << tservice->get_name() << " */";
    block_open(out);
    out << endl;

    indent(out) << "let __inProtocol : TProtocol" << endl << endl;
    indent(out) << "let __outProtocol : TProtocol" << endl << endl;
    indent(out) << "public init(inoutProtocol: TProtocol)";
    block_open(out);

    indent(out) << "__inProtocol = inoutProtocol" << endl;
    indent(out) << "__outProtocol = inoutProtocol" << endl;
    block_close(out);
    out << endl;

    indent(out) << "public init(inProtocol: TProtocol, outProtocol: TProtocol)";
    block_open(out);
    indent(out) << "__inProtocol = inProtocol" << endl;
    indent(out) << "__outProtocol = outProtocol" << endl;
    block_close(out);
    out << endl;
  }

  block_close(out);
  out << endl;
}

/**
 * Generates a service client interface definition.
 *
 * @param tservice The service to generate a client interface definition for
 */
void t_swift_generator::generate_swift_service_client_async(ostream& out, t_service* tservice) {
  if (!gen_cocoa_) {
    indent(out) << "open class " << tservice->get_name()
                << "AsyncClient<Protocol: TProtocol, Factory: TAsyncTransportFactory>";// : "

    // Inherit from ParentClient
    t_service* parent = tservice->get_extends();

    out << " : " << ((parent == NULL) ? "T" :  parent->get_name()) + "AsyncClient<Protocol, Factory>";
    out <<  " /* , " << tservice->get_name() << " */";

    block_open(out);
    out << endl;
  } else {
    indent(out) << "public class " << tservice->get_name() << "AsyncClient /* : " << tservice->get_name() << " */";
    block_open(out);
    out << endl;

    indent(out) << "let __protocolFactory : TProtocolFactory" << endl << endl;
    indent(out) << "let __transportFactory : TAsyncTransportFactory" << endl << endl;
    indent(out) << "public init(protocolFactory: TProtocolFactory, transportFactory: TAsyncTransportFactory)";
    block_open(out);

    indent(out) << "__protocolFactory = protocolFactory" << endl;
    indent(out) << "__transportFactory = transportFactory" << endl;
    block_close(out);
    out << endl;
  }
  block_close(out);
  out << endl;
}

/**
 * Generates a service server interface definition. In other words,
 * the TProcess implementation for the service definition.
 *
 * @param tservice The service to generate a client interface definition for
 */
void t_swift_generator::generate_swift_service_server(ostream& out, t_service* tservice) {
  if (!gen_cocoa_) {
    indent(out) << "open class " << tservice->get_name() << "Processor /* " << tservice->get_name() << " */";

    block_open(out);
    out << endl;
    out << indent() << "typealias ProcessorHandlerDictionary = "
        << "[String: (Int32, TProtocol, TProtocol, " << tservice->get_name() << ") throws -> Void]" << endl
        << endl
        << indent() << "public var service: " << tservice->get_name() << endl
        << endl
        << indent() << "public required init(service: " << tservice->get_name() << ")";
  } else {
    indent(out) << "public class " << tservice->get_name() << "Processor : NSObject /* "
                << tservice->get_name() << " */";
    block_open(out);
    out << endl;

    out << indent() << "typealias ProcessorHandlerDictionary = "
        << "[String: (Int, TProtocol, TProtocol, " << tservice->get_name() << ") throws -> Void]" << endl
        << endl
        << indent() << "let service : " << tservice->get_name() << endl
        << endl
        << indent() << "public init(service: " << tservice->get_name() << ")";
  }

  block_open(out);
  indent(out) << "self.service = service" << endl;
  block_close(out);
  out << endl;

  block_close(out);
  out << endl;
}

/**
 * Generates a function that will send the arguments
 * for a service function via a protocol.
 *
 * @param tservice  The service to generate
 * @param tfunction The function to generate
 * @param needs_protocol
 *                  Wether the first parameter must be a protocol or if
 *                  the protocol is to be assumed
 */
void t_swift_generator::generate_swift_service_client_send_function_implementation(ostream& out,
                                                                                   t_service *tservice,
                                                                                   t_function* tfunction,
                                                                                   bool needs_protocol) {

  string funname = tfunction->get_name();

  t_function send_function(g_type_bool,
                           "send_" + tfunction->get_name(),
                           tfunction->get_arglist());

  string argsname = function_args_helper_struct_type(tservice, tfunction);
  t_struct* arg_struct = tfunction->get_arglist();

  string proto = needs_protocol ? (gen_cocoa_ ? "__outProtocol" : "on outProtocol") : "";
  // Open function
  indent(out) << "private func " << send_function.get_name() << "("
              << argument_list(tfunction->get_arglist(), proto, true)
              << ") throws";
  block_open(out);
  if (!gen_cocoa_) {
    // Serialize the request
    indent(out) << "try outProtocol.writeMessageBegin(name: \"" << funname << "\", "
                << "type: " << (tfunction->is_oneway() ? ".oneway" : ".call") << ", "
                << "sequenceID: 0)" << endl;

    indent(out) << "let args = " << argsname << "(";

    // write out function parameters

    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator f_iter;

    for (f_iter = fields.begin(); f_iter != fields.end();) {
      t_field *tfield = (*f_iter);
      out << tfield->get_name() << ": " << tfield->get_name();
      if (++f_iter != fields.end()) {
        out << ", ";
      }
    }
    out << ")" << endl;
    indent(out) << "try args.write(to: outProtocol)" << endl;
    indent(out) << "try outProtocol.writeMessageEnd()" << endl;
  } else {
    out << endl;

    // Serialize the request
    indent(out) << "try __outProtocol.writeMessageBeginWithName(\"" << funname << "\", "
                << "type: " << (tfunction->is_oneway() ? ".ONEWAY" : ".CALL") << ", "
                << "sequenceID: 0)" << endl;

    out << endl;

    indent(out) << "let __args = " << argsname << "(";

    // write out function parameters

    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator f_iter;

    for (f_iter = fields.begin(); f_iter != fields.end();) {
      t_field *tfield = (*f_iter);
      out << tfield->get_name() << ": " << tfield->get_name();
      if (++f_iter != fields.end()) {
        out << ", ";
      }
    }
    out << ")" << endl;
    indent(out) << "try " << argsname << ".writeValue(__args, toProtocol: __outProtocol)" << endl << endl;
    indent(out) << "try __outProtocol.writeMessageEnd()" << endl;
  }

  block_close(out);
  out << endl;
}

/**
 * Generates a function that will recv the result for a
 * service function via a protocol.
 *
 * @param tservice  The service to generate
 * @param tfunction The function to generate
 * @param needs_protocol
 *                  Wether the first parameter must be a protocol or if
 *                  the protocol is to be assumed
 */
void t_swift_generator::generate_swift_service_client_recv_function_implementation(ostream& out,
                                                                                   t_service* tservice,
                                                                                   t_function* tfunction,
                                                                                   bool needs_protocol) {

  // Open function
  indent(out) << "private func recv_" << tfunction->get_name() << "(";
  if (!gen_cocoa_) {
    if (needs_protocol) {
      out << "on inProtocol: TProtocol";
    }
    out << ") throws";
    if (!tfunction->get_returntype()->is_void()) {
      out << " -> " << type_name(tfunction->get_returntype());
    }

    block_open(out);

    // check for an exception

    indent(out) << "try inProtocol.readResultMessageBegin() " << endl;

    string resultname = function_result_helper_struct_type(tservice, tfunction);
    indent(out);
    if (!tfunction->get_returntype()->is_void() || !tfunction->get_xceptions()->get_members().empty()) {
      out << "let result = ";
    } else {
      out << "_ = ";
    }

    string return_type_name = type_name(tfunction->get_returntype());
    out << "try " << resultname << ".read(from: inProtocol)" << endl;

    indent(out) << "try inProtocol.readMessageEnd()" << endl << endl;

    // Careful, only return _result if not a void function
    if (!tfunction->get_returntype()->is_void()) {
      indent(out) << "if let success = result.success";
      block_open(out);
      indent(out) << "return success" << endl;
      block_close(out);
    }

    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& xceptions = xs->get_members();
    vector<t_field*>::const_iterator x_iter;

    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      indent(out) << "if let " << (*x_iter)->get_name() << " = result." << (*x_iter)->get_name();
      block_open(out);
      indent(out) << "throw " << (*x_iter)->get_name() << endl;
      block_close(out);
    }

    // If you get here it's an exception, unless a void function
    if (!tfunction->get_returntype()->is_void()) {
      indent(out) << "throw TApplicationError(error: .missingResult(methodName: \""
                  << tfunction->get_name() << "\"))" << endl;
    }
  } else {
    if (needs_protocol) {
      out << "__inProtocol: TProtocol";
    }

    out << ") throws";

    if (!tfunction->get_returntype()->is_void()) {
      out << " -> " << type_name(tfunction->get_returntype());
    }

    block_open(out);

    // check for an exception
    out << endl;
    indent(out) << "try __inProtocol.readResultMessageBegin() " << endl << endl;
    string resultname = function_result_helper_struct_type(tservice, tfunction);
    indent(out);
    if (!tfunction->get_returntype()->is_void() || !tfunction->get_xceptions()->get_members().empty()) {
      out << "let __result = ";
    }
    out << "try " << resultname << ".readValueFromProtocol(__inProtocol)" << endl << endl;

    indent(out) << "try __inProtocol.readMessageEnd()" << endl << endl;

    // Careful, only return _result if not a void function
    if (!tfunction->get_returntype()->is_void()) {
      indent(out) << "if let __success = __result.success";
      block_open(out);
      indent(out) << "return __success" << endl;
      block_close(out);
    }

    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& xceptions = xs->get_members();
    vector<t_field*>::const_iterator x_iter;

    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      indent(out) << "if let " << (*x_iter)->get_name() << " = __result." << (*x_iter)->get_name();
      block_open(out);
      indent(out) << "throw " << (*x_iter)->get_name() << endl;
      block_close(out);
    }

    // If you get here it's an exception, unless a void function
    if (!tfunction->get_returntype()->is_void()) {
      indent(out) << "throw NSError(" << endl;
      indent_up();
      indent(out) << "domain: TApplicationErrorDomain, " << endl;
      indent(out) << "code: Int(TApplicationError.MissingResult.rawValue)," << endl;
      indent(out) << "userInfo: [TApplicationErrorMethodKey: \"" << tfunction->get_name() << "\"])" << endl;
      indent_down();
    }
  }

  // Close function
  block_close(out);
  out << endl;
}

/**
 * Generates an invocation of a given the send function for the
 * service function.
 *
 * @param tfunction The service to generate an implementation for
 */
void t_swift_generator::generate_swift_service_client_send_function_invocation(ostream& out,
                                                                               t_function* tfunction) {

  indent(out) << "try send_" << tfunction->get_name() << "(";

  t_struct* arg_struct = tfunction->get_arglist();

  const vector<t_field*>& fields = arg_struct->get_members();
  vector<t_field*>::const_iterator f_iter;

  for (f_iter = fields.begin(); f_iter != fields.end();) {
    out << (*f_iter)->get_name() << ": " << (*f_iter)->get_name();
    if (++f_iter != fields.end()) {
      out << ", ";
    }
  }

  out << ")" << endl;
}

/**
 * Generates an invocation of a given the send function for the
 * service function. This is for asynchronous protocols.
 *
 * @param tfunction The service to generate an implementation for
 */
void t_swift_generator::generate_swift_service_client_send_async_function_invocation(ostream& out,
                                                                                     t_function* tfunction) {

  t_struct* arg_struct = tfunction->get_arglist();
  const vector<t_field*>& fields = arg_struct->get_members();
  vector<t_field*>::const_iterator f_iter;

  if (!gen_cocoa_) {
    indent(out) << "try send_" << tfunction->get_name() << "(on: proto";
  } else {
    indent(out) << "try send_" << tfunction->get_name() << "(__protocol"; //
  }

  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    out << ", " << (*f_iter)->get_name() << ": " << (*f_iter)->get_name();
  }

  out << ")" << endl;
}

/**
 * Generates a service client protocol implementation via extension.
 *
 * @param tservice The service to generate an implementation for
 */
void t_swift_generator::generate_swift_service_client_implementation(ostream& out,
                                                                     t_service* tservice) {

  string name = tservice->get_name() + "Client";
  indent(out) << "extension " << name << " : " << tservice->get_name();
  block_open(out);
  out << endl;

  // generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {

    generate_swift_service_client_send_function_implementation(out, tservice, *f_iter, false);

    if (!(*f_iter)->is_oneway()) {
      generate_swift_service_client_recv_function_implementation(out, tservice, *f_iter, false);
    }

    // Open function
    indent(out) << "public " << function_signature(*f_iter);
    block_open(out);

    if (gen_cocoa_) { out << endl; }

    generate_swift_service_client_send_function_invocation(out, *f_iter);
    if (!gen_cocoa_) {
      indent(out) << "try outProtocol.transport.flush()" << endl;
    } else {
      out << endl;
      indent(out) << "try __outProtocol.transport().flush()" << endl << endl;
    }

    if (!(*f_iter)->is_oneway()) {
      if ((*f_iter)->get_returntype()->is_void()) {
        indent(out) << "try recv_" << (*f_iter)->get_name() << "()" << endl;
      } else {
        indent(out) << "return try recv_" << (*f_iter)->get_name() << "()" << endl;
      }
    }
    block_close(out);
    out << endl;
  }
  block_close(out);
  out << endl;
}

/**
 * Generates a service asynchronous client protocol implementation via extension.
 *
 * @param tservice The service to generate an implementation for
 */
void t_swift_generator::generate_swift_service_client_async_implementation(ostream& out, t_service* tservice) {
  if (gen_cocoa_) {
    generate_old_swift_service_client_async_implementation(out, tservice);
    return;
  }
  string name = tservice->get_name() + "AsyncClient";
  string protocol_name = tservice->get_name() + "Async";

  indent(out) << "extension " << name << " : " << protocol_name;
  block_open(out);
  out << endl;

  // generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {

    generate_swift_service_client_send_function_implementation(out, tservice, *f_iter, true);

    if (!(*f_iter)->is_oneway()) {
      generate_swift_service_client_recv_function_implementation(out, tservice, *f_iter, true);
    }

    indent(out) << "public " << async_function_signature(*f_iter);
    block_open(out);
    out << endl;
    out << indent() << "let transport   = factory.newTransport()" << endl
        << indent() << "let proto = Protocol(on: transport)" << endl
        << endl;

    out << indent() << "do";
    block_open(out);

    generate_swift_service_client_send_async_function_invocation(out, *f_iter);

    indent_down();
    out << indent() << "} catch let error {" << endl;
    indent_up();
    out << indent() << "completion(.error(error))" << endl;
    block_close(out);

    out << endl;

    bool ret_is_void = (*f_iter)->get_returntype()->is_void();
    bool is_oneway = (*f_iter)->is_oneway();

    string error_completion_call = "completion(.error(error))";
    indent(out) << "transport.flush";
    block_open(out);
    out << indent() << "(trans, error) in" << endl << endl;
    out << indent() << "if let error = error";
    block_open(out);
    out << indent() << error_completion_call << endl;
    block_close(out);

    if (!is_oneway) {
      out << indent() << "do";
      block_open(out);
      indent(out);
      if (!ret_is_void) {
        out << "let result = ";
      }
      out << "try self.recv_" << (*f_iter)->get_name() << "(on: proto)" << endl;

      out << indent() << (ret_is_void ? "completion(.success(Void()))" : "completion(.success(result))") << endl;
      indent_down();
      out << indent() << "} catch let error {" << endl;
      indent_up();
      out << indent() << error_completion_call << endl;

      block_close(out);
    } else {
      out << indent() << "completion(.success(Void()))" << endl;
    }
    block_close(out);
    block_close(out);

  }
  block_close(out);
  out << endl;
}

void t_swift_generator::generate_old_swift_service_client_async_implementation(ostream& out,
                                                                               t_service* tservice) {

  string name = tservice->get_name() + "AsyncClient";
  string protocol_name = tservice->get_name() + "Async";

  indent(out) << "extension " << name << " : " << protocol_name;
  block_open(out);
  out << endl;

  // generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {

    generate_swift_service_client_send_function_implementation(out, tservice, *f_iter, true);

    if (!(*f_iter)->is_oneway()) {
      generate_swift_service_client_recv_function_implementation(out, tservice, *f_iter, true);
    }

    indent(out) << "public " << async_function_signature(*f_iter);
    block_open(out);
    out << endl;

    out << indent() << "let __transport = __transportFactory.newTransport()" << endl
        << indent() << "let __protocol = __protocolFactory.newProtocolOnTransport(__transport)" << endl
        << endl;

    generate_swift_service_client_send_async_function_invocation(out, *f_iter);
    out << endl;

    indent(out) << "__transport.flushWithCompletion(";

    if ((*f_iter)->is_oneway()) {
      out << "success, failure: failure)" << endl;
    }
    else {
      block_open(out);
      indent(out) << "do";
      block_open(out);

      indent(out);
      if (!(*f_iter)->get_returntype()->is_void()) {
        out << "let result = ";
      }
      out << "try self.recv_" << (*f_iter)->get_name() << "(__protocol)" << endl;

      out << indent() << "success(";
      if (!(*f_iter)->get_returntype()->is_void()) {
        out << "result";
      }
      out << ")" << endl;

      block_close(out);
      indent(out) << "catch let error";
      block_open(out);
      indent(out) << "failure(error as NSError)" << endl;
      block_close(out);
      block_close(out);
      indent(out) << ", failure: failure)" << endl;
    }

    block_close(out);
    out << endl;

    // Promise function
    if (promise_kit_) {

      indent(out) << "public " << promise_function_signature(*f_iter);
      block_open(out);

      out << indent() << "let (__promise, __fulfill, __reject) = Promise<" << type_name((*f_iter)->get_returntype()) << ">.pendingPromise()" << endl << endl
          << indent() << "let __transport = __transportFactory.newTransport()" << endl
          << indent() << "let __protocol = __protocolFactory.newProtocolOnTransport(__transport)" << endl
          << endl;

      generate_swift_service_client_send_async_function_invocation(out, *f_iter);
      out << endl;
      indent(out) << "__transport.flushWithCompletion(";

      if ((*f_iter)->is_oneway()) {
        out << "{ __fulfill() }, failure: { __reject($0) })" << endl;
      }
      else {
        block_open(out);
        indent(out) << "do";
        block_open(out);

        indent(out);
        if (!(*f_iter)->get_returntype()->is_void()) {
          out << "let result = ";
        }
        out << "try self.recv_" << (*f_iter)->get_name() << "(__protocol)" << endl;

        out << indent() << "__fulfill(";
        if (!(*f_iter)->get_returntype()->is_void()) {
          out << "result";
        }
        out << ")" << endl;

        block_close(out);
        indent(out) << "catch let error";
        block_open(out);
        indent(out) << "__reject(error)" << endl;
        block_close(out);
        block_close(out);

        indent(out) << ", failure: { error in " << endl;
        indent_up();
        indent(out) << "__reject(error)" << endl;
        indent_down();
        indent(out) << "})" << endl;
      }

      indent(out) << "return __promise" << endl;
      block_close(out);
      out << endl;

    }

  }
  block_close(out);
  out << endl;
}

/**
 * Generates a service server implementation.
 *
 * Implemented by generating a block for each service function that
 * handles the processing of that function. The blocks are stored in
 * a map and looked up via function/message name.
 *
 * @param tservice The service to generate an implementation for
 */
void t_swift_generator::generate_swift_service_server_implementation(ostream& out,
                                                                     t_service* tservice) {

  string name = tservice->get_name() + "Processor";

  indent(out) << "extension " << name << " : TProcessor";
  block_open(out);
  out << endl;
  indent(out) << "static let processorHandlers" << (gen_cocoa_ ? " " : "") << ": ProcessorHandlerDictionary =";
  block_open(out);

  out << endl;
  out << indent() << "var processorHandlers = ProcessorHandlerDictionary()" << endl << endl;

  // generate method map for routing incoming calls
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {

    t_function* tfunction = *f_iter;

    string args_type = function_args_helper_struct_type(tservice, *f_iter);

    out << indent() << "processorHandlers[\"" << tfunction->get_name() << "\"] = { sequenceID, inProtocol, outProtocol, handler in" << endl
        << endl;

    indent_up();
    if (!gen_cocoa_) {
      out << indent() << "let args = try " << args_type << ".read(from: inProtocol)" << endl
          << endl
          << indent() << "try inProtocol.readMessageEnd()" << endl
          << endl;
    } else {
      out << indent() << "let args = try " << args_type << ".readValueFromProtocol(inProtocol)" << endl
          << endl
          << indent() << "try inProtocol.readMessageEnd()" << endl
          << endl;
    }

    if (!tfunction->is_oneway() ) {
      string result_type = function_result_helper_struct_type(tservice, tfunction);
      indent(out) << "var result = " << result_type << "()" << endl;

      indent(out) << "do";
      block_open(out);

      indent(out);
      if (!tfunction->get_returntype()->is_void()) {
        out << "result.success = ";
      }
      out << "try handler." << (gen_cocoa_ ? function_name(tfunction) : tfunction->get_name()) << "(";

      t_struct* arg_struct = tfunction->get_arglist();
      const vector<t_field*>& fields = arg_struct->get_members();
      vector<t_field*>::const_iterator f_iter;

      for (f_iter = fields.begin(); f_iter != fields.end();) {
        string fieldName = (*f_iter)->get_name();
        if (!gen_cocoa_ || f_iter != fields.begin()) {
          out << fieldName << ": ";
        }

        out << "args." << fieldName;
        if (++f_iter != fields.end()) {
          out << ", ";
        }
      }

      out << ")" << endl;
      block_close(out);

      t_struct* xs = tfunction->get_xceptions();
      const vector<t_field*>& xfields = xs->get_members();
      vector<t_field*>::const_iterator x_iter;

      if (!gen_cocoa_) {
        for (x_iter = xfields.begin(); x_iter != xfields.end(); ++x_iter) {
          indent(out) << "catch let error as ";

          t_program* program = (*x_iter)->get_type()->get_program();
          if ((*x_iter)->get_type()->get_name() == "Error" && namespaced_ && program != program_) {
            out << get_real_swift_module(program) << ".";
          }
          out << (*x_iter)->get_type()->get_name();

          out << " { result." << (*x_iter)->get_name() << " = error }" << endl;
        }

        indent(out) << "catch let error { throw error }" << endl;
        out << endl;

        if (!tfunction->is_oneway()) {
          out << indent() << "try outProtocol.writeMessageBegin(name: \"" << tfunction->get_name() << "\", type: .reply, sequenceID: sequenceID)" << endl
              << indent() << "try result.write(to: outProtocol)" << endl
              << indent() << "try outProtocol.writeMessageEnd()" << endl;
        }
      } else {
        for (x_iter = xfields.begin(); x_iter != xfields.end(); ++x_iter) {
          indent(out) << "catch let error as " << (*x_iter)->get_type()->get_name();
          block_open(out);
          indent(out) << "result." << (*x_iter)->get_name() << " = error" << endl;
          block_close(out);
        }

        indent(out) << "catch let error";
        block_open(out);
        out << indent() << "throw error" << endl;
        block_close(out);

        out << endl;

        if (!tfunction->is_oneway()) {
          out << indent() << "try outProtocol.writeMessageBeginWithName(\"" << tfunction->get_name() << "\", type: .REPLY, sequenceID: sequenceID)" << endl
              << indent() << "try " << result_type << ".writeValue(result, toProtocol: outProtocol)" << endl
              << indent() << "try outProtocol.writeMessageEnd()" << endl;
        }
      }
    }
    block_close(out);

  }

  indent(out) << "return processorHandlers" << endl;

  block_close(out,false);
  out << "()" << endl;
  out << endl;

  if (!gen_cocoa_) {
    indent(out) << "public func process(on inProtocol: TProtocol, outProtocol: TProtocol) throws";
  } else {
    indent(out) << "public func processOnInputProtocol(inProtocol: TProtocol, outputProtocol outProtocol: TProtocol) throws";
  }
  block_open(out);

  out << endl;
  out << indent() << "let (messageName, _, sequenceID) = try inProtocol.readMessageBegin()" << endl
      << endl
      << indent() << "if let processorHandler = " << name << ".processorHandlers[messageName]";
  block_open(out);
  out << indent() << "do";
  block_open(out);
  out << indent() << "try processorHandler(sequenceID, inProtocol, outProtocol, service)" << endl;
  block_close(out);
  if (!gen_cocoa_) {
    out << indent() << "catch let error as TApplicationError";
    block_open(out);
    out << indent() << "try outProtocol.writeException(messageName: messageName, sequenceID: sequenceID, ex: error)" << endl;
    block_close(out);
    block_close(out);
    out << indent() << "else";
    block_open(out);
    out << indent() << "try inProtocol.skip(type: .struct)" << endl
        << indent() << "try inProtocol.readMessageEnd()" << endl
        << indent() << "let ex = TApplicationError(error: .unknownMethod(methodName: messageName))" << endl
        << indent() << "try outProtocol.writeException(messageName: messageName, "
        << "sequenceID: sequenceID, ex: ex)" << endl;
  } else {
    out << indent() << "catch let error as NSError";
    block_open(out);
    out << indent() << "try outProtocol.writeExceptionForMessageName(messageName, sequenceID: sequenceID, ex: error)" << endl;
    block_close(out);
    block_close(out);
    out << indent() << "else";
    block_open(out);
    out << indent() << "try inProtocol.skipType(.STRUCT)" << endl
        << indent() << "try inProtocol.readMessageEnd()" << endl
        << indent() << "try outProtocol.writeExceptionForMessageName(messageName," << endl;
    indent_up();
    out << indent() << "sequenceID: sequenceID," << endl
        << indent() << "ex: NSError(" << endl;
    indent_up();
    out << indent() << "domain: TApplicationErrorDomain, " << endl
        << indent() << "code: Int(TApplicationError.UnknownMethod.rawValue), " << endl
        << indent() << "userInfo: [TApplicationErrorMethodKey: messageName]))" << endl;
    indent_down();
    indent_down();
  }

  block_close(out);
  block_close(out);
  block_close(out);
  out << endl;
}

/**
 * Returns an Swift name
 *
 * @param ttype The type
 * @param class_ref Do we want a Class reference istead of a type reference?
 * @return Swift type name, i.e. Dictionary<Key,Value>
 */
string t_swift_generator::type_name(t_type* ttype, bool is_optional, bool is_forced) {
  string result = "";

  if (ttype->is_base_type()) {
    result += base_type_name((t_base_type*)ttype);
  } else if (ttype->is_map()) {
    t_map *map = (t_map *)ttype;
    result += "TMap<" + type_name(map->get_key_type()) + ", " + type_name(map->get_val_type()) + ">";
  } else if (ttype->is_set()) {
    t_set *set = (t_set *)ttype;
    result += "TSet<" + type_name(set->get_elem_type()) + ">";
  } else if (ttype->is_list()) {
    t_list *list = (t_list *)ttype;
    result += "TList<" + type_name(list->get_elem_type()) + ">";
  }
  else {
    t_program* program = ttype->get_program();
    if (namespaced_ && program != program_) {
      result += get_real_swift_module(program) + ".";
    }
    result += ttype->get_name();
  }

  if (is_optional) {
    result += "?";
  }
  if (is_forced) {
    result += "!";
  }

  return result;
}

/**
 * Returns the Swift type that corresponds to the thrift type.
 *
 * @param tbase The base type
 */
string t_swift_generator::base_type_name(t_base_type* type) {
  t_base_type::t_base tbase = type->get_base();

  switch (tbase) {
  case t_base_type::TYPE_VOID:
    return "Void";
  case t_base_type::TYPE_STRING:
    if (type->is_binary()) {
      return gen_cocoa_ ? "TBinary" : "Data";
    } else {
      return "String";
    }
  case t_base_type::TYPE_BOOL:
    return "Bool";
  case t_base_type::TYPE_I8:
    return "Int8";
  case t_base_type::TYPE_I16:
    return "Int16";
  case t_base_type::TYPE_I32:
    return "Int32";
  case t_base_type::TYPE_I64:
    return "Int64";
  case t_base_type::TYPE_DOUBLE:
    return "Double";
  default:
    throw "compiler error: no Swift name for base type " + t_base_type::t_base_name(tbase);
  }
}

/**
 * Renders full constant value (as would be seen after an '=')
 *
 */
void t_swift_generator::render_const_value(ostream& out,
                                           t_type* type,
                                           t_const_value* value) {
  type = get_true_type(type);

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_STRING:
      out << "\"" << get_escaped_string(value) << "\"";
      break;
    case t_base_type::TYPE_BOOL:
      out << ((value->get_integer() > 0) ? "true" : "false");
      break;
    case t_base_type::TYPE_I8:
    case t_base_type::TYPE_I16:
    case t_base_type::TYPE_I32:
    case t_base_type::TYPE_I64:
      out << type_name(type) << "(" << value->get_integer() << ")";
      break;
    case t_base_type::TYPE_DOUBLE:
      out << type_name(type) << "(";
      if (value->get_type() == t_const_value::CV_INTEGER) {
        out << value->get_integer();
      } else {
        out << value->get_double();
      }
      out << ")";
      break;
    default:
      throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
    }
  } else if (type->is_enum()) {
    out << (gen_cocoa_ ? value->get_identifier() : enum_const_name(value->get_identifier())); // Swift2/Cocoa compatibility
  } else if (type->is_struct() || type->is_xception()) {

    out << type_name(type) << "(";

    const vector<t_field*>& fields = ((t_struct*)type)->get_members();
    vector<t_field*>::const_iterator f_iter;

    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;

    for (f_iter = fields.begin(); f_iter != fields.end();) {
      t_field* tfield = *f_iter;
      t_const_value* value = NULL;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        if (tfield->get_name() == v_iter->first->get_string()) {
          value = v_iter->second;
        }
      }

      if (value) {
        out << tfield->get_name() << ": ";
        render_const_value(out, tfield->get_type(), value);
      }
      else if (!field_is_optional(tfield)) {
        throw "constant error: required field " + type->get_name() + "." + tfield->get_name() + " has no value";
      }

      if (++f_iter != fields.end()) {
        out << ", ";
      }
    }

    out << ")";

  } else if (type->is_map()) {

    out << "[";

    t_type* ktype = ((t_map*)type)->get_key_type();
    t_type* vtype = ((t_map*)type)->get_val_type();

    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;

    for (v_iter = val.begin(); v_iter != val.end();) {

      render_const_value(out, ktype, v_iter->first);
      out << ": ";
      render_const_value(out, vtype, v_iter->second);

      if (++v_iter != val.end()) {
        out << ", ";
      }
    }

    out << "]";

  } else if (type->is_list()) {

    out << "[";

    t_type* etype = ((t_list*)type)->get_elem_type();

    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;

    for (v_iter = val.begin(); v_iter != val.end();) {

      render_const_value(out, etype, v_iter->first);

      if (++v_iter != val.end()) {
        out << ", ";
      }
    }

    out << "]";

  } else if (type->is_set()) {

    out << "[";

    t_type* etype = ((t_set*)type)->get_elem_type();

    const map<t_const_value*, t_const_value*, t_const_value::value_compare>& val = value->get_map();
    map<t_const_value*, t_const_value*, t_const_value::value_compare>::const_iterator v_iter;

    for (v_iter = val.begin(); v_iter != val.end();) {

      render_const_value(out, etype, v_iter->first);

      if (++v_iter != val.end()) {
        out << ", ";
      }
    }

    out << "]";

  } else {
    throw "compiler error: no const of type " + type->get_name();
  }

}

/**
 * Declares an Swift property.
 *
 * @param tfield The field to declare a property for
 */
string t_swift_generator::declare_property(t_field* tfield, bool is_private) {

  string visibility = is_private ? (gen_cocoa_ ? "private" : "fileprivate") : "public";

  ostringstream render;

  render << visibility << " var " << maybe_escape_identifier(tfield->get_name());

  if (field_is_optional(tfield)) {
    render << (gen_cocoa_ ? " " : "") << ": " << type_name(tfield->get_type(), true);
  }
  else {
    if (!gen_cocoa_) {
      render << ": " << type_name(tfield->get_type(), false);
    } else {
      // Swift2/Cocoa backward compat, Bad, default init
      render << " = " << type_name(tfield->get_type(), false) << "()";
    }
  }

  return render.str();
}

/**
 * Renders a function signature
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_swift_generator::function_signature(t_function* tfunction) {

  string result = "func " + (gen_cocoa_ ? function_name(tfunction) : tfunction->get_name());

  result += "(" + argument_list(tfunction->get_arglist(), "", false) + ") throws"; /// argsreview

  t_type* ttype = tfunction->get_returntype();
  if (!ttype->is_void()) {
    result += " -> " + type_name(ttype);
  }

  return result;
}

/**
 * Renders a function docstring
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
void t_swift_generator::function_docstring(ostream& out, t_function* tfunction) {

    // Generate docstring with following format:
    // /// <Description>
    // /// <empty line>
    // /// - Parameters:
    // ///   - <parameter>: <parameter docstring>
    // /// - Returns: <return type> (Thrift has no docstring on return val)
    // /// - Throws: <exception types>

    // Description
    string doc = tfunction->get_doc();
    generate_docstring(out, doc);
    indent(out) << "///" << endl;

    // Parameters
    const vector<t_field*>& fields = tfunction->get_arglist()->get_members();
    vector<t_field*>::const_iterator f_iter;
    if (!fields.empty()) {
      indent(out) << "/// - Parameters:" << endl;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        indent(out) << "///   - " << (*f_iter)->get_name() << ": ";
        string doc = (*f_iter)->get_doc();
        if (!doc.empty() && doc[doc.length()-1] == '\n') {
            doc.erase(doc.length()-1);
        }
        out << doc << endl;
      }
    }

    // Returns
    t_type* ttype = tfunction->get_returntype();
    if (!ttype->is_void()) {
      indent(out) << "/// - Returns: " << type_name(ttype) << endl;
    }

    // Throws
    indent(out) << "/// - Throws: ";
    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& xceptions = xs->get_members();
    vector<t_field*>::const_iterator x_iter;
    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      out << type_name((*x_iter)->get_type());
      if (*x_iter != xceptions.back()) {
        out << ", ";
      }    }
    out << endl;
}

/**
 * Renders a function docstring
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
void t_swift_generator::async_function_docstring(ostream& out, t_function* tfunction) {
    // Generate docstring with following format:
    // /// <Description>
    // /// <empty line>
    // /// - Parameters:
    // ///   - <parameter>: <parameter docstring>
    // ///   - callback: <callback types>

    // Description
    string doc = tfunction->get_doc();
    generate_docstring(out, doc);
    indent(out) << "///" << endl;

    // Parameters
    const vector<t_field*>& fields = tfunction->get_arglist()->get_members();
    vector<t_field*>::const_iterator f_iter;
    if (!fields.empty()) {
      indent(out) << "/// - Parameters:" << endl;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        indent(out) << "///   - " << (*f_iter)->get_name() << ": ";
        string doc = (*f_iter)->get_doc();
        if (!doc.empty() && doc[doc.length()-1] == '\n') {
            doc.erase(doc.length()-1);
        }
        out << doc << endl;
      }
    }

    // completion
    indent(out) << "///   - completion: TAsyncResult<" << type_name(tfunction->get_returntype())
                << "> wrapping return and following Exceptions: ";
    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& xceptions = xs->get_members();
    vector<t_field*>::const_iterator x_iter;
    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      out << type_name((*x_iter)->get_type());
      if (*x_iter != xceptions.back()) {
        out << ", ";
      }
    }
    out << endl;
}

/**
 * Renders a function signature that returns asynchronously via blocks.
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_swift_generator::async_function_signature(t_function* tfunction) {
  t_type* ttype = tfunction->get_returntype();
  t_struct* targlist = tfunction->get_arglist();
  string result = "func " + (gen_cocoa_ ? function_name(tfunction) : tfunction->get_name());

  if (!gen_cocoa_) {
    string response_string = "(TAsyncResult<";
    response_string += (ttype->is_void()) ? "Void" : type_name(ttype);
    response_string += ">) -> Void";
    result += "(" + argument_list(tfunction->get_arglist(), "", false)
            + (targlist->get_members().size() ? ", " : "")
            + "completion: @escaping " + response_string + ")";
  } else {
    string response_param = "(" + ((ttype->is_void()) ? "" : type_name(ttype)) + ") -> Void";
    result += "(" + argument_list(tfunction->get_arglist(), "", false)
            + (targlist->get_members().size() ? ", " : "")
            + "success: " + response_param + ", "
            + "failure: (NSError) -> Void) throws";
  }
  return result;
}

/**
 * Renders a function signature that returns asynchronously via promises.
 * ONLY FOR Swift2/Cocoa BACKWARDS COMPATIBILITY
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_swift_generator::promise_function_signature(t_function* tfunction) {
  return "func " + function_name(tfunction) + "(" + argument_list(tfunction->get_arglist(), "", false) + ") throws "
         + "-> Promise<" + type_name(tfunction->get_returntype()) + ">";
}

/**
 * Renders a verbose function name suitable for a Swift method.  ONLY FOR Swift2/Cocoa BACKWARDS COMPATIBILITY
 */
string t_swift_generator::function_name(t_function* tfunction) {
  string name = tfunction->get_name();
  if (!tfunction->get_arglist()->get_members().empty()) {
    string first_arg = tfunction->get_arglist()->get_members().front()->get_name();
    if (name.size() < first_arg.size() ||
        lowercase(name.substr(name.size()-first_arg.size())) != lowercase(first_arg)) {
      name += "With" + capitalize(tfunction->get_arglist()->get_members()[0]->get_name());
    }
  }
  return name;
}

/**
 * Renders a Swift method argument list
 */
string t_swift_generator::argument_list(t_struct* tstruct, string protocol_name, bool is_internal) {
  string result = "";
  bool include_protocol = !protocol_name.empty();

  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;

  if (include_protocol) {
    result += protocol_name + ": TProtocol";
    if (!fields.empty()) {
      result += ", ";
    }
  } else if (!fields.empty() && is_internal && gen_cocoa_) {
    // Force first argument to be named, Swift2/Cocoa backwards compat
    result += fields.front()->get_name() + " ";
  }

  for (f_iter = fields.begin(); f_iter != fields.end();) {
    t_field* arg = *f_iter;

    if (!gen_cocoa_) {
      // optional args not usually permitted for some reason, even though dynamic langs handle it
      // use annotation "swift.nullable" to achieve
      result += arg->get_name() + ": " + type_name(arg->get_type(), field_is_optional(arg));
    } else {
      result += arg->get_name() + ": " + type_name(arg->get_type());
    }

    if (++f_iter != fields.end()) {
      result += ", ";
    }
  }
  return result;
}

/**
 * https://developer.apple.com/library/ios/documentation/Swift/Conceptual/Swift_Programming_Language/LexicalStructure.html
 *
 */

void t_swift_generator::populate_reserved_words() {
  if (!gen_cocoa_) {
    swift_reserved_words_.insert("__COLUMN__");
    swift_reserved_words_.insert("__FILE__");
    swift_reserved_words_.insert("__FUNCTION__");
    swift_reserved_words_.insert("__LINE__");
    swift_reserved_words_.insert("Any");
    swift_reserved_words_.insert("as");
    swift_reserved_words_.insert("associatedtype");
    swift_reserved_words_.insert("associativity");
    swift_reserved_words_.insert("break");
    swift_reserved_words_.insert("case");
    swift_reserved_words_.insert("catch");
    swift_reserved_words_.insert("class");
    swift_reserved_words_.insert("continue");
    swift_reserved_words_.insert("convenience");
    swift_reserved_words_.insert("default");
    swift_reserved_words_.insert("defer");
    swift_reserved_words_.insert("deinit");
    swift_reserved_words_.insert("didSet");
    swift_reserved_words_.insert("do");
    swift_reserved_words_.insert("dynamic");
    swift_reserved_words_.insert("dynamicType");
    swift_reserved_words_.insert("else");
    swift_reserved_words_.insert("enum");
    swift_reserved_words_.insert("extension");
    swift_reserved_words_.insert("fallthrough");
    swift_reserved_words_.insert("false");
    swift_reserved_words_.insert("fileprivate");
    swift_reserved_words_.insert("final");
    swift_reserved_words_.insert("for");
    swift_reserved_words_.insert("func");
    swift_reserved_words_.insert("get");
    swift_reserved_words_.insert("guard");
    swift_reserved_words_.insert("if");
    swift_reserved_words_.insert("import");
    swift_reserved_words_.insert("in");
    swift_reserved_words_.insert("indirect");
    swift_reserved_words_.insert("infix");
    swift_reserved_words_.insert("init");
    swift_reserved_words_.insert("inout");
    swift_reserved_words_.insert("internal");
    swift_reserved_words_.insert("is");
    swift_reserved_words_.insert("lazy");
    swift_reserved_words_.insert("left");
    swift_reserved_words_.insert("let");
    swift_reserved_words_.insert("mutating");
    swift_reserved_words_.insert("nil");
    swift_reserved_words_.insert("none");
    swift_reserved_words_.insert("nonmutating");
    swift_reserved_words_.insert("open");
    swift_reserved_words_.insert("operator");
    swift_reserved_words_.insert("optional");
    swift_reserved_words_.insert("override");
    swift_reserved_words_.insert("postfix");
    swift_reserved_words_.insert("precedence");
    swift_reserved_words_.insert("prefix");
    swift_reserved_words_.insert("private");
    swift_reserved_words_.insert("protocol");
    swift_reserved_words_.insert("Protocol");
    swift_reserved_words_.insert("public");
    swift_reserved_words_.insert("repeat");
    swift_reserved_words_.insert("required");
    swift_reserved_words_.insert("rethrows");
    swift_reserved_words_.insert("return");
    swift_reserved_words_.insert("right");
    swift_reserved_words_.insert("self");
    swift_reserved_words_.insert("Self");
    swift_reserved_words_.insert("set");
    swift_reserved_words_.insert("static");
    swift_reserved_words_.insert("struct");
    swift_reserved_words_.insert("subscript");
    swift_reserved_words_.insert("super");
    swift_reserved_words_.insert("switch");
    swift_reserved_words_.insert("throw");
    swift_reserved_words_.insert("throws");
    swift_reserved_words_.insert("true");
    swift_reserved_words_.insert("try");
    swift_reserved_words_.insert("Type");
    swift_reserved_words_.insert("typealias");
    swift_reserved_words_.insert("unowned");
    swift_reserved_words_.insert("var");
    swift_reserved_words_.insert("weak");
    swift_reserved_words_.insert("where");
    swift_reserved_words_.insert("while");
    swift_reserved_words_.insert("willSet");
  } else {
    swift_reserved_words_.insert("Self");
    swift_reserved_words_.insert("associatedtype");
    swift_reserved_words_.insert("defer");
    swift_reserved_words_.insert("deinit");
    swift_reserved_words_.insert("dynamicType");
    swift_reserved_words_.insert("enum");
    swift_reserved_words_.insert("extension");
    swift_reserved_words_.insert("fallthrough");
    swift_reserved_words_.insert("false");
    swift_reserved_words_.insert("func");
    swift_reserved_words_.insert("guard");
    swift_reserved_words_.insert("init");
    swift_reserved_words_.insert("inout");
    swift_reserved_words_.insert("internal");
    swift_reserved_words_.insert("let");
    swift_reserved_words_.insert("operator");
    swift_reserved_words_.insert("protocol");
    swift_reserved_words_.insert("repeat");
    swift_reserved_words_.insert("rethrows");
    swift_reserved_words_.insert("struct");
    swift_reserved_words_.insert("subscript");
    swift_reserved_words_.insert("throws");
    swift_reserved_words_.insert("true");
    swift_reserved_words_.insert("typealias");
    swift_reserved_words_.insert("where");
  }
}

string t_swift_generator::maybe_escape_identifier(const string& identifier) {
  if (swift_reserved_words_.find(identifier) != swift_reserved_words_.end()) {
    return "`" + identifier + "`";
  }
  return identifier;
}

/**
 * Converts the parse type to a Swift TType enumeration.
 */
string t_swift_generator::type_to_enum(t_type* type, bool qualified) {
  type = get_true_type(type);

  string result = qualified ? "TType." : ".";
  if (!gen_cocoa_) {
    if (type->is_base_type()) {
      t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
      switch (tbase) {
        case t_base_type::TYPE_VOID:
          throw "NO T_VOID CONSTRUCT";
        case t_base_type::TYPE_STRING:
          return result + "string";
        case t_base_type::TYPE_BOOL:
          return result + "bool";
        case t_base_type::TYPE_I8:
          return result + "i8";
        case t_base_type::TYPE_I16:
          return result + "i16";
        case t_base_type::TYPE_I32:
          return result + "i32";
        case t_base_type::TYPE_I64:
          return result + "i64";
        case t_base_type::TYPE_DOUBLE:
          return result + "double";
      }
    } else if (type->is_enum()) {
      return result + "i32";
    } else if (type->is_struct() || type->is_xception()) {
      return result + "struct";
    } else if (type->is_map()) {
      return result + "map";
    } else if (type->is_set()) {
      return result + "set";
    } else if (type->is_list()) {
      return result + "list";
    }
  } else {
    if (type->is_base_type()) {
      t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
      switch (tbase) {
        case t_base_type::TYPE_VOID:
          throw "NO T_VOID CONSTRUCT";
        case t_base_type::TYPE_STRING:
          return result + "STRING";
        case t_base_type::TYPE_BOOL:
          return result + "BOOL";
        case t_base_type::TYPE_I8:
          return result + "BYTE";
        case t_base_type::TYPE_I16:
          return result + "I16";
        case t_base_type::TYPE_I32:
          return result + "I32";
        case t_base_type::TYPE_I64:
          return result + "I64";
        case t_base_type::TYPE_DOUBLE:
          return result + "DOUBLE";
      }
    } else if (type->is_enum()) {
      return result + "I32";
    } else if (type->is_struct() || type->is_xception()) {
      return result + "STRUCT";
    } else if (type->is_map()) {
      return result + "MAP";
    } else if (type->is_set()) {
      return result + "SET";
    } else if (type->is_list()) {
      return result + "LIST";
    }
  }

  throw "INVALID TYPE IN type_to_enum: " + type->get_name();
}


THRIFT_REGISTER_GENERATOR(
    swift,
    "Swift 3.0",
    "    log_unexpected:  Log every time an unexpected field ID or type is encountered.\n"
    "    debug_descriptions:\n"
    "                     Allow use of debugDescription so the app can add description via a cateogory/extension\n"
    "    async_clients:   Generate clients which invoke asynchronously via block syntax.\n"
    "    namespaced:      Generate source in Module scoped output directories for Swift Namespacing.\n"
    "    cocoa:           Generate Swift 2.x code compatible with the Thrift/Cocoa library\n"
    "    promise_kit:     Generate clients which invoke asynchronously via promises (only use with cocoa flag)\n"
    "    safe_enums:      Generate enum types with an unknown case to handle unspecified values rather than throw a serialization error\n")
