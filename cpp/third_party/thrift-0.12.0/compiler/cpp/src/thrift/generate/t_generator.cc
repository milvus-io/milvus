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

#include "thrift/generate/t_generator.h"
using namespace std;

/**
 * Top level program generation function. Calls the generator subclass methods
 * for preparing file streams etc. then iterates over all the parts of the
 * program to perform the correct actions.
 *
 * @param program The thrift program to compile into C++ source
 */
void t_generator::generate_program() {
  // Initialize the generator
  init_generator();

  // Generate enums
  vector<t_enum*> enums = program_->get_enums();
  vector<t_enum*>::iterator en_iter;
  for (en_iter = enums.begin(); en_iter != enums.end(); ++en_iter) {
    generate_enum(*en_iter);
  }

  // Generate typedefs
  vector<t_typedef*> typedefs = program_->get_typedefs();
  vector<t_typedef*>::iterator td_iter;
  for (td_iter = typedefs.begin(); td_iter != typedefs.end(); ++td_iter) {
    generate_typedef(*td_iter);
  }

  // Generate structs, exceptions, and unions in declared order
  vector<t_struct*> objects = program_->get_objects();

  vector<t_struct*>::iterator o_iter;
  for (o_iter = objects.begin(); o_iter != objects.end(); ++o_iter) {
    generate_forward_declaration(*o_iter);
  }
  for (o_iter = objects.begin(); o_iter != objects.end(); ++o_iter) {
    if ((*o_iter)->is_xception()) {
      generate_xception(*o_iter);
    } else {
      generate_struct(*o_iter);
    }
  }

  // Generate constants
  vector<t_const*> consts = program_->get_consts();
  generate_consts(consts);

  // Generate services
  vector<t_service*> services = program_->get_services();
  vector<t_service*>::iterator sv_iter;
  for (sv_iter = services.begin(); sv_iter != services.end(); ++sv_iter) {
    service_name_ = get_service_name(*sv_iter);
    generate_service(*sv_iter);
  }

  // Close the generator
  close_generator();
}

std::set<std::string> t_generator::lang_keywords() const {
  std::string keywords[] = { "BEGIN", "END", "__CLASS__", "__DIR__", "__FILE__", "__FUNCTION__",
      "__LINE__", "__METHOD__", "__NAMESPACE__", "abstract", "alias", "and", "args", "as",
      "assert", "begin", "break", "case", "catch", "class", "clone", "continue", "declare",
      "def", "default", "del", "delete", "do", "dynamic", "elif", "else", "elseif", "elsif",
      "end", "enddeclare", "endfor", "endforeach", "endif", "endswitch", "endwhile", "ensure",
      "except", "exec", "finally", "float", "for", "foreach", "from", "function", "global",
      "goto", "if", "implements", "import", "in", "inline", "instanceof", "interface", "is",
      "lambda", "module", "native", "new", "next", "nil", "not", "or", "package", "pass",
      "public", "print", "private", "protected", "raise", "redo", "rescue", "retry", "register",
      "return", "self", "sizeof", "static", "super", "switch", "synchronized", "then", "this",
      "throw", "transient", "try", "undef", "unless", "unsigned", "until", "use", "var",
      "virtual", "volatile", "when", "while", "with", "xor", "yield" };
  return std::set<std::string>(keywords, keywords + sizeof(keywords)/sizeof(keywords[0]) );
}

void t_generator::validate_input() const {
  validate(program_->get_enums());
  validate(program_->get_typedefs());
  validate(program_->get_objects());
  validate(program_->get_consts());
  validate(program_->get_services());
}

template <typename T>
void t_generator::validate(const vector<T>& list) const{
  typename vector<T>::const_iterator it;
  for(it=list.begin(); it != list.end(); ++it) {
      validate(*it);
  }
}

void t_generator::validate(t_function const* f) const {
  validate_id(f->get_name());
  validate(f->get_arglist());
  validate(f->get_xceptions());
}

void t_generator::validate(t_service const* s) const {
  validate_id(s->get_name());
  validate(s->get_functions());
}

void t_generator::validate(t_enum const* en) const {
  validate_id(en->get_name());
  validate(en->get_constants());
}
void t_generator::validate(t_struct const* s) const {
  validate_id(s->get_name());
  validate(s->get_members());
}

void t_generator::validate(t_enum_value const* en_val) const {
  validate_id(en_val->get_name());
}
void t_generator::validate(t_typedef const* td) const {
  validate_id(td->get_name());
}
void t_generator::validate(t_const const* c) const {
  validate_id(c->get_name());
}
void t_generator::validate(t_field const* f) const {
  validate_id(f->get_name());
}

void t_generator::validate_id(const string& id) const {
  if (keywords_.find(id) != keywords_.end()) {
    failure("Cannot use reserved language keyword: \"%s\"", id.c_str());
  }
}

string t_generator::escape_string(const string& in) const {
  string result = "";
  for (string::const_iterator it = in.begin(); it < in.end(); it++) {
    std::map<char, std::string>::const_iterator res = escape_.find(*it);
    if (res != escape_.end()) {
      result.append(res->second);
    } else {
      result.push_back(*it);
    }
  }
  return result;
}

void t_generator::generate_consts(vector<t_const*> consts) {
  vector<t_const*>::iterator c_iter;
  for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter) {
    generate_const(*c_iter);
  }
}

void t_generator::generate_docstring_comment(ostream& out,
                                             const string& comment_start,
                                             const string& line_prefix,
                                             const string& contents,
                                             const string& comment_end) {
  if (!comment_start.empty())
    indent(out) << comment_start;
  stringstream docs(contents, ios_base::in);
  while (!(docs.eof() || docs.fail())) {
    char line[1024];
    docs.getline(line, 1024);

    if (strlen(line) > 0) {
      indent(out) << line_prefix << line << std::endl;
    } else if (line_prefix.empty()){
      out << std::endl;
    } else if(!docs.eof()) {
      indent(out) << line_prefix << std::endl;
    }
  }
  if (!comment_end.empty())
    indent(out) << comment_end;
}

void t_generator_registry::register_generator(t_generator_factory* factory) {
  gen_map_t& the_map = get_generator_map();
  if (the_map.find(factory->get_short_name()) != the_map.end()) {
    failure("Duplicate generators for language \"%s\"!\n", factory->get_short_name().c_str());
  }
  the_map[factory->get_short_name()] = factory;
}

void t_generator::parse_options(const string& options,
                                string& language,
                                map<string, string>& parsed_options) {
  string::size_type colon = options.find(':');
  language = options.substr(0, colon);

  if (colon != string::npos) {
    string::size_type pos = colon + 1;
    while (pos != string::npos && pos < options.size()) {
      string::size_type next_pos = options.find(',', pos);
      string option = options.substr(pos, next_pos - pos);
      pos = ((next_pos == string::npos) ? next_pos : next_pos + 1);

      string::size_type separator = option.find('=');
      string key, value;
      if (separator == string::npos) {
        key = option;
        value = "";
      } else {
        key = option.substr(0, separator);
        value = option.substr(separator + 1);
      }

      parsed_options[key] = value;
    }
  }
}

t_generator* t_generator_registry::get_generator(t_program* program,
                                                 const string& language,
                                                 const map<string, string>& parsed_options,
                                                 const std::string& options) {
  gen_map_t& the_map = get_generator_map();
  gen_map_t::iterator iter = the_map.find(language);

  if (iter == the_map.end()) {
    return NULL;
  }

  return iter->second->get_generator(program, parsed_options, options);
}

t_generator* t_generator_registry::get_generator(t_program* program, const string& options) {
  string language;
  map<string, string> parsed_options;
  t_generator::parse_options(options, language, parsed_options);
  return get_generator(program, language, parsed_options, options);
}

t_generator_registry::gen_map_t& t_generator_registry::get_generator_map() {
  // http://www.parashift.com/c++-faq-lite/ctors.html#faq-10.12
  static gen_map_t* the_map = new gen_map_t();
  return *the_map;
}

t_generator_factory::t_generator_factory(const std::string& short_name,
                                         const std::string& long_name,
                                         const std::string& documentation)
  : short_name_(short_name), long_name_(long_name), documentation_(documentation) {
  t_generator_registry::register_generator(this);
}
