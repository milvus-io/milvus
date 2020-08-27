//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#pragma once

#include	"NGT/Index.h"

namespace NGT {


class Command {
public:
  class SearchParameter {
  public:
    SearchParameter() {
      openMode   = 'r';
      query      = "";
      querySize  = 0;
      indexType  = 't';
      size       = 20;
      edgeSize   = -1;
      outputMode = "-";
      radius     = FLT_MAX;
      step	 = 0;
      trial      = 1;
      beginOfEpsilon = endOfEpsilon = stepOfEpsilon = 0.1;
      accuracy	 = 0.0;
    }
    SearchParameter(Args &args) { parse(args); }
    void parse(Args &args) {
      openMode = args.getChar("m", 'r');
      try {
	query = args.get("#2");
      } catch (...) {
	NGTThrowException("ngt: Error: Query is not specified");
      }
      querySize = args.getl("Q", 0);
      indexType	= args.getChar("i", 't');
      size	= args.getl("n", 20);
      // edgeSize
      // -1(default) : using the size which was specified at the index creation.
      //  0 : no limitation for the edge size.
      // -2('e') : automatically set it according to epsilon.
      if (args.getChar("E", '-') == 'e') {
	edgeSize	= -2;
      } else {
	edgeSize	= args.getl("E", -1);
      }
      outputMode	= args.getString("o", "-");
      radius		= args.getf("r", FLT_MAX);
      trial		= args.getl("t", 1);
      {
	beginOfEpsilon = endOfEpsilon = stepOfEpsilon = 0.1;
	std::string epsilon = args.getString("e", "0.1");
	std::vector<std::string> tokens;
	NGT::Common::tokenize(epsilon, tokens, ":");
	if (tokens.size() >= 1) { beginOfEpsilon = endOfEpsilon = NGT::Common::strtod(tokens[0]); }
	if (tokens.size() >= 2) { endOfEpsilon = NGT::Common::strtod(tokens[1]); }
	if (tokens.size() >= 3) { stepOfEpsilon = NGT::Common::strtod(tokens[2]); }
	step = 0;
	if (tokens.size() >= 4) { step = NGT::Common::strtol(tokens[3]); }
      }
      accuracy		= args.getf("a", 0.0);
    }
    char	openMode;
    std::string	query;
    size_t	querySize;
    char	indexType;
    int		size;
    long	edgeSize;
    std::string	outputMode;
    float	radius;
    float	beginOfEpsilon;
    float	endOfEpsilon;
    float	stepOfEpsilon;
    float	accuracy;
    size_t	step;
    size_t	trial;
  };

  Command():debugLevel(0) {}

  void create(Args &args);
  void append(Args &args);
  static void search(NGT::Index &index, SearchParameter &searchParameter, std::ostream &stream)
  {
    std::ifstream		is(searchParameter.query);
    if (!is) {
      std::cerr << "Cannot open the specified file. " << searchParameter.query << std::endl;
      return;
    }
    search(index, searchParameter, is, stream);
  }
  static void search(NGT::Index &index, SearchParameter &searchParameter, std::istream &is, std::ostream &stream);
  void search(Args &args);
  void remove(Args &args);
  void exportIndex(Args &args);
  void importIndex(Args &args);
  void prune(Args &args);
  void reconstructGraph(Args &args);
  void optimizeSearchParameters(Args &args);
  void optimizeNumberOfEdgesForANNG(Args &args);
  void refineANNG(Args &args);
  void repair(Args &args);

  void info(Args &args);
  void setDebugLevel(int level) { debugLevel = level; }
  int getDebugLevel() { return debugLevel; }

protected:
  int debugLevel;

};

}; // NGT
