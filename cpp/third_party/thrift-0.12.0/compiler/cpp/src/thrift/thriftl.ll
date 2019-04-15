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

/**
 * Thrift scanner.
 *
 * Tokenizes a thrift definition file.
 */

%{

/* This is redundant with some of the flags in Makefile.am, but it works
 * when people override CXXFLAGS without being careful. The pragmas are
 * the 'right' way to do it, but don't work on old-enough GCC (in particular
 * the GCC that ship on Mac OS X 10.6.5, *counter* to what the GNU docs say)
 *
 * We should revert the Makefile.am changes once Apple ships a reasonable
 * GCC.
 */
#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-label"
#endif

#ifdef _MSC_VER
#pragma warning( push )

// warning C4102: 'find_rule' : unreferenced label
#pragma warning( disable : 4102 )

// warning C4267: 'argument' : conversion from 'size_t' to 'int', possible loss of data
#pragma warning( disable : 4267 )

// avoid isatty redefinition
#define YY_NEVER_INTERACTIVE 1

#define YY_NO_UNISTD_H 1
#endif

#include <cassert>
#include <string>
#include <errno.h>
#include <stdlib.h>

#ifdef _MSC_VER
#include "thrift/windows/config.h"
#endif
#include "thrift/main.h"
#include "thrift/common.h"
#include "thrift/globals.h"
#include "thrift/parse/t_program.h"

/**
 * Must be included AFTER parse/t_program.h, but I can't remember why anymore
 * because I wrote this a while ago.
 */
#if defined(BISON_USE_PARSER_H_EXTENSION)
#include "thrift/thrifty.h"
#else
#include "thrift/thrifty.hh"
#endif

void integer_overflow(char* text) {
  yyerror("This integer is too big: \"%s\"\n", text);
  exit(1);
}

void unexpected_token(char* text) {
  yyerror("Unexpected token in input: \"%s\"\n", text);
  exit(1);
}

%}

/**
 * Provides the yylineno global, useful for debugging output
 */
%option lex-compat

/**
 * Our inputs are all single files, so no need for yywrap
 */
%option noyywrap

/**
 * We don't use it, and it fires up warnings at -Wall
 */
%option nounput

/**
 * Helper definitions, comments, constants, and whatnot
 */

intconstant   ([+-]?[0-9]+)
hexconstant   ([+-]?"0x"[0-9A-Fa-f]+)
dubconstant   ([+-]?[0-9]*(\.[0-9]+)?([eE][+-]?[0-9]+)?)
identifier    ([a-zA-Z_](\.[a-zA-Z_0-9]|[a-zA-Z_0-9])*)
whitespace    ([ \t\r\n]*)
sillycomm     ("/*""*"*"*/")
multicm_begin ("/*")
doctext_begin ("/**")
comment       ("//"[^\n]*)
unixcomment   ("#"[^\n]*)
symbol        ([:;\,\{\}\(\)\=<>\[\]])
literal_begin (['\"])

%%

{whitespace}         { /* do nothing */                 }
{sillycomm}          { /* do nothing */                 }

{doctext_begin} {
  std::string parsed("/**");
  int state = 0;  // 0 = normal, 1 = "*" seen, "*/" seen
  while(state < 2)
  {
    int ch = yyinput();
    parsed.push_back(ch);
    switch (ch) {
      case EOF:
        yyerror("Unexpected end of file in doc-comment at %d\n", yylineno);
        exit(1);
      case '*':
        state = 1;
        break;
      case '/':
        state = (state == 1) ? 2 : 0;
        break;
      default:
        state = 0;
        break;
    }
  }
  pdebug("doctext = \"%s\"\n",parsed.c_str());

 /* This does not show up in the parse tree. */
 /* Rather, the parser will grab it out of the global. */
  if (g_parse_mode == PROGRAM) {
    clear_doctext();
    g_doctext = strdup(parsed.c_str() + 3);
    assert(strlen(g_doctext) >= 2);
    g_doctext[strlen(g_doctext) - 2] = ' ';
    g_doctext[strlen(g_doctext) - 1] = '\0';
    g_doctext = clean_up_doctext(g_doctext);
    g_doctext_lineno = yylineno;
    if( (g_program_doctext_candidate == NULL) && (g_program_doctext_status == INVALID)){
      g_program_doctext_candidate = strdup(g_doctext);
      g_program_doctext_lineno = g_doctext_lineno;
      g_program_doctext_status = STILL_CANDIDATE;
      pdebug("%s","program doctext set to STILL_CANDIDATE");
    }
  }
}

{multicm_begin}  { /* parsed, but thrown away */
  std::string parsed("/*");
  int state = 0;  // 0 = normal, 1 = "*" seen, "*/" seen
  while(state < 2)
  {
    int ch = yyinput();
    parsed.push_back(ch);
    switch (ch) {
      case EOF:
        yyerror("Unexpected end of file in multiline comment at %d\n", yylineno);
        exit(1);
      case '*':
        state = 1;
        break;
      case '/':
        state = (state == 1) ? 2 : 0;
        break;
      default:
        state = 0;
        break;
    }
  }
  pdebug("multi_comm = \"%s\"\n",parsed.c_str());
}

{comment}            { /* do nothing */                 }
{unixcomment}        { /* do nothing */                 }

{symbol}             { return yytext[0];                }
"*"                  { return yytext[0];                }

"false"              { yylval.iconst=0; return tok_int_constant; }
"true"               { yylval.iconst=1; return tok_int_constant; }

"namespace"          { return tok_namespace;            }
"cpp_namespace"      { error_unsupported_namespace_decl("cpp"); /* do nothing */ }
"cpp_include"        { return tok_cpp_include;          }
"cpp_type"           { return tok_cpp_type;             }
"java_package"       { error_unsupported_namespace_decl("java_package", "java"); /* do nothing */ }
"cocoa_prefix"       { error_unsupported_namespace_decl("cocoa_prefix", "cocoa"); /* do nothing */ }
"csharp_namespace"   { error_unsupported_namespace_decl("csharp"); /* do nothing */ }
"delphi_namespace"   { error_unsupported_namespace_decl("delphi"); /* do nothing */ }
"php_namespace"      { error_unsupported_namespace_decl("php"); /* do nothing */ }
"py_module"          { error_unsupported_namespace_decl("py_module", "py"); /* do nothing */ }
"perl_package"       { error_unsupported_namespace_decl("perl_package", "perl"); /* do nothing */ }
"ruby_namespace"     { error_unsupported_namespace_decl("ruby"); /* do nothing */ }
"smalltalk_category" { error_unsupported_namespace_decl("smalltalk_category", "smalltalk.category"); /* do nothing */ }
"smalltalk_prefix"   { error_unsupported_namespace_decl("smalltalk_category", "smalltalk.category"); /* do nothing */ }
"xsd_all"            { return tok_xsd_all;              }
"xsd_optional"       { return tok_xsd_optional;         }
"xsd_nillable"       { return tok_xsd_nillable;         }
"xsd_namespace"      { error_unsupported_namespace_decl("xsd"); /* do nothing */ }
"xsd_attrs"          { return tok_xsd_attrs;            }
"include"            { return tok_include;              }
"void"               { return tok_void;                 }
"bool"               { return tok_bool;                 }
"byte"               {
  emit_byte_type_warning();
  return tok_i8;
}
"i8"                 { return tok_i8;                   }
"i16"                { return tok_i16;                  }
"i32"                { return tok_i32;                  }
"i64"                { return tok_i64;                  }
"double"             { return tok_double;               }
"string"             { return tok_string;               }
"binary"             { return tok_binary;               }
"slist" {
  pwarning(0, "\"slist\" is deprecated and will be removed in a future compiler version.  This type should be replaced with \"string\".\n");
  return tok_slist;
}
"senum" {
  pwarning(0, "\"senum\" is deprecated and will be removed in a future compiler version.  This type should be replaced with \"string\".\n");
  return tok_senum;
}
"map"                { return tok_map;                  }
"list"               { return tok_list;                 }
"set"                { return tok_set;                  }
"oneway"             { return tok_oneway;               }
"typedef"            { return tok_typedef;              }
"struct"             { return tok_struct;               }
"union"              { return tok_union;                }
"exception"          { return tok_xception;             }
"extends"            { return tok_extends;              }
"throws"             { return tok_throws;               }
"service"            { return tok_service;              }
"enum"               { return tok_enum;                 }
"const"              { return tok_const;                }
"required"           { return tok_required;             }
"optional"           { return tok_optional;             }
"async" {
  pwarning(0, "\"async\" is deprecated.  It is called \"oneway\" now.\n");
  return tok_oneway;
}
"&"                  { return tok_reference;            }

{intconstant} {
  errno = 0;
  yylval.iconst = strtoll(yytext, NULL, 10);
  if (errno == ERANGE) {
    integer_overflow(yytext);
  }
  return tok_int_constant;
}

{hexconstant} {
  errno = 0;
  char sign = yytext[0];
  int shift = sign == '0' ? 2 : 3;
  yylval.iconst = strtoll(yytext+shift, NULL, 16);
  if (sign == '-') {
    yylval.iconst = -yylval.iconst;
  }
  if (errno == ERANGE) {
    integer_overflow(yytext);
  }
  return tok_int_constant;
}

{identifier} {
  yylval.id = strdup(yytext);
  return tok_identifier;
}

{dubconstant} {
 /* Deliberately placed after identifier, since "e10" is NOT a double literal (THRIFT-3477) */
  yylval.dconst = atof(yytext);
  return tok_dub_constant;
}

{literal_begin} {
  char mark = yytext[0];
  std::string result;
  for(;;)
  {
    int ch = yyinput();
    switch (ch) {
      case EOF:
        yyerror("End of file while read string at %d\n", yylineno);
        exit(1);
      case '\n':
        yyerror("End of line while read string at %d\n", yylineno - 1);
        exit(1);
      case '\\':
        ch = yyinput();
        switch (ch) {
          case 'r':
            result.push_back('\r');
            continue;
          case 'n':
            result.push_back('\n');
            continue;
          case 't':
            result.push_back('\t');
            continue;
          case '"':
            result.push_back('"');
            continue;
          case '\'':
            result.push_back('\'');
            continue;
          case '\\':
            result.push_back('\\');
            continue;
          default:
            yyerror("Bad escape character\n");
            return -1;
        }
        break;
      default:
        if (ch == mark) {
          yylval.id = strdup(result.c_str());
          return tok_literal;
        } else {
          result.push_back(ch);
        }
    }
  }
}


. {
  unexpected_token(yytext);
}

%%

#ifdef _MSC_VER
#pragma warning( pop )
#endif

/* vim: filetype=lex
*/
