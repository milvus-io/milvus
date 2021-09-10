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

#include	"Command.h"


#define NGT_LOG_BASED_OPTIMIZATION

namespace NGT {
  class Optimizer {
  public:


    Optimizer(NGT::Index &i, size_t n = 10):index(i), nOfResults(n) { 
    }
    ~Optimizer() {}

    class MeasuredValue {
    public:
    MeasuredValue():keyValue(0.0), totalCount(0), meanAccuracy(0.0), meanTime(0.0), meanDistanceCount(0.0), meanVisitCount(0.0) {}
      double	keyValue;
      size_t	totalCount;
      float	meanAccuracy;
      double	meanTime;
      double	meanDistanceCount;
      double	meanVisitCount;
    };

    class SumupValues {
    public:
      class Result {
      public:
	size_t queryNo;
	double key;
	double accuracy;
	double time;
	double distanceCount;
	double visitCount;
	double meanDistance;
	std::vector<size_t> searchedIDs;
	std::vector<size_t> unsearchedIDs;
      };

      SumupValues(bool res = false):resultIsAvailable(res){}

      void clear() {
	totalAccuracy.clear();
	totalTime.clear();
	totalDistanceCount.clear();
	totalVisitCount.clear();
	totalCount.clear();
      }

      std::vector<MeasuredValue> sumup() {
	std::vector<MeasuredValue> accuracies;
	for (auto it = totalAccuracy.begin(); it != totalAccuracy.end(); ++it) {
	  MeasuredValue a;
	  a.keyValue = (*it).first;
	  a.totalCount = totalCount[a.keyValue];
	  a.meanAccuracy = totalAccuracy[a.keyValue] / (double)totalCount[a.keyValue];
	  a.meanTime = totalTime[a.keyValue] / (double)totalCount[a.keyValue];
	  a.meanDistanceCount = (double)totalDistanceCount[a.keyValue] / (double)totalCount[a.keyValue];
	  a.meanVisitCount = (double)totalVisitCount[a.keyValue] / (double)totalCount[a.keyValue];
	  accuracies.push_back(a);
	}
	return accuracies;
      }
      
      std::map<double, double> totalAccuracy;
      std::map<double, double> totalTime;
      std::map<double, size_t> totalDistanceCount;
      std::map<double, size_t> totalVisitCount;
      std::map<double, size_t> totalCount;

      bool resultIsAvailable;
      std::vector<Result> results;

    };

    void enableLog() { redirector.disable(); }
    void disableLog() { redirector.enable(); }

    static void search(NGT::Index &index, std::istream &gtStream, Command::SearchParameter &sp, std::vector<MeasuredValue> &acc) {
      std::ifstream		is(sp.query);
      if (!is) {
	std::stringstream msg;
	msg << "Cannot open the specified file. " << sp.query;
	NGTThrowException(msg);
      }

      search(index, gtStream, sp, acc);
    }

    static void search(NGT::Index &index, std::istream &queries, std::istream &gtStream, Command::SearchParameter &sp, std::vector<MeasuredValue> &acc) {
      sp.stepOfEpsilon = 1.0;
      std::stringstream resultStream;
      NGT::Command::search(index, sp, queries, resultStream);
      resultStream.clear();
      resultStream.seekg(0, std::ios_base::beg);
      std::string type;
      size_t actualResultSize = 0;
      gtStream.seekg(0, std::ios_base::end);      
      auto pos = gtStream.tellg();
      if (pos == 0) {
	acc = evaluate(resultStream, type, actualResultSize);
      } else {
	SumupValues sumupValues(true); 
	gtStream.clear();
	gtStream.seekg(0, std::ios_base::beg);
	acc = evaluate(gtStream, resultStream, sumupValues, type, actualResultSize);

      }

      assert(acc.size() == 1);
    }

    static std::vector<MeasuredValue>
      evaluate(std::istream &resultStream, std::string &type, 
	       size_t &resultDataSize, size_t specifiedResultSize = 0, size_t groundTruthSize = 0, bool recall = false)
    {

      resultDataSize = 0;

      if (recall) {
	if (specifiedResultSize == 0) {
	  std::stringstream msg;
	  msg << "For calculating recalls, the result size should be specified.";
	  NGTThrowException(msg);
	}
	resultDataSize = specifiedResultSize;
      } else {
	checkAndGetSize(resultStream, resultDataSize);
      }

      std::string line;
      size_t queryNo = 1;

      SumupValues	sumupValues;

      resultStream.clear();
      resultStream.seekg(0, std::ios_base::beg);

      do {
	std::unordered_set<size_t> gt;
	double farthestDistance = 0.0;
	sumup(resultStream, queryNo, sumupValues,
	      gt, resultDataSize, type, recall, farthestDistance);
	queryNo++;
      } while (!resultStream.eof());

      return sumupValues.sumup();
    }

    static std::vector<MeasuredValue>
      evaluate(std::istream &gtStream, std::istream &resultStream, std::string &type, 
	       size_t &resultDataSize, size_t specifiedResultSize = 0, size_t groundTruthSize = 0, bool recall = false)
    {
      SumupValues sumupValues;
      return evaluate(gtStream, resultStream, sumupValues, type, resultDataSize, specifiedResultSize, groundTruthSize, recall);
    }

    static std::vector<MeasuredValue>
      evaluate(std::istream &gtStream, std::istream &resultStream, SumupValues &sumupValues, std::string &type, 
	       size_t &resultDataSize, size_t specifiedResultSize = 0, size_t groundTruthSize = 0, bool recall = false)
    {
      resultDataSize = 0;

      if (recall) {
	if (specifiedResultSize == 0) {
	  std::stringstream msg;
	  msg << "For calculating recalls, the result size should be specified.";
	  NGTThrowException(msg);
	}
	resultDataSize = specifiedResultSize;
      } else {
	checkAndGetSize(resultStream, resultDataSize);
      }

      std::string line;
      size_t queryNo = 1;
      sumupValues.clear();

      resultStream.clear();
      resultStream.seekg(0, std::ios_base::beg);

      while (getline(gtStream, line)) {
	std::vector<std::string> tokens;
	NGT::Common::tokenize(line, tokens, "=");
	if (tokens.size() == 0) {
	  continue;
	}
	if (tokens[0] == "# Query No.") {
	  if (tokens.size() > 1 && (size_t)NGT::Common::strtol(tokens[1]) == queryNo) {
	    std::unordered_set<size_t> gt;
	    double farthestDistance;
	    if (groundTruthSize == 0) {
	      loadGroundTruth(gtStream, gt, resultDataSize, farthestDistance);
	    } else {
	      loadGroundTruth(gtStream, gt, groundTruthSize, farthestDistance);
	    }
	    sumup(resultStream, queryNo, sumupValues,
		  gt, resultDataSize, type, recall, farthestDistance);

	    queryNo++;
	  }
	}
      }

      return sumupValues.sumup();
    }

    static void
      loadGroundTruth(std::istream & gtf, std::unordered_set<size_t> & gt, size_t resultDataSize, double &distance) {
      std::string line;
      size_t dataCount = 0;
      size_t searchCount = 0;
      while (getline(gtf, line)) {
	if (line.size() != 0 && line.at(0) == '#') {
	  std::vector<std::string> gtf;
	  NGT::Common::tokenize(line, gtf, "=");
	  if (gtf.size() >= 1) {
	    if (gtf[0] == "# End of Search") {
	      searchCount++;
	    }
	    if (gtf[0] == "# End of Query") {
	      if (searchCount != 1) {
		std::stringstream msg;
		msg << "Error: gt has not just one search result.";
		NGTThrowException(msg);
	      }
	      if (dataCount < resultDataSize) {
		std::stringstream msg;
		msg << "Error: gt data is less than result size! " << dataCount << ":" << resultDataSize;
		NGTThrowException(msg);
	      }
	      return;
	    }
	    continue;
	  }
	}
	dataCount++;
	if (dataCount > resultDataSize) {
	  continue;
	}
	std::vector<std::string> result;      
	NGT::Common::tokenize(line, result, " \t");
	if (result.size() < 3) {
	  std::stringstream msg;
	  msg << "result format is wrong. ";
	  NGTThrowException(msg);
	}
	size_t id = NGT::Common::strtol(result[1]);
	distance = NGT::Common::strtod(result[2]);
	try {
	  gt.insert(id);
	} catch(...) {
	  std::stringstream msg;
	  msg << "Cannot insert id into the gt. " << id;
	  NGTThrowException(msg);
	}
      } 
    }

    static void checkAndGetSize(std::istream &resultStream, size_t &resultDataSize)
    {
      size_t lineCount = 0;
      size_t prevDataCount = 0;
      std::string line;
      bool warn = false;

      while (getline(resultStream, line)) {
	lineCount++;
	if (line.size() != 0 && line.at(0) == '#') {
	  std::vector<std::string> tf;
	  NGT::Common::tokenize(line, tf, "=");
	  if (tf.size() >= 1 && tf[0] == "# Query No.") {
	    size_t dataCount = 0;
	    std::string lastDataLine;
	    while (getline(resultStream, line)) {
	      lineCount++;
	      if (line.size() != 0 && line.at(0) == '#') {
		std::vector<std::string> gtf;
		NGT::Common::tokenize(line, gtf, "=");
		if (gtf.size() >= 1 && gtf[0] == "# End of Search") {
		  if (prevDataCount == 0) {
		    prevDataCount = dataCount;
		  } else {
		    if (prevDataCount != dataCount) {
		      warn = true;
		      std::cerr << "Warning!: Result sizes are inconsistent! $prevDataCount:$dataCount" << std::endl;
		      std::cerr << "  Line No." << lineCount << ":"  << lastDataLine << std::endl;
		      if (prevDataCount < dataCount) {
			prevDataCount = dataCount;
		      }
		    }
		  }
		  dataCount = 0;
		  break;
		}
		continue;
	      }
	      lastDataLine = line;
	      std::vector<std::string> result;      
	      NGT::Common::tokenize(line, result, " \t");
	      if (result.size() < 3) {
		std::stringstream msg;
		msg << "result format is wrong. ";
		NGTThrowException(msg);
	      }
	      size_t rank = NGT::Common::strtol(result[0]);
	      dataCount++;
	      if (rank != dataCount) {
		std::stringstream msg;
		msg << "check: inner error! " << rank << ":" << dataCount;
		NGTThrowException(msg);
	      }
	    }
	  }
	}
      }
      resultDataSize = prevDataCount;
      if (warn) {
	std::cerr << "Warning! ****************************************************************************" << std::endl;
	std::cerr << " Check if the result number $$resultDataSize is correct." << std::endl;
	std::cerr << "Warning! ****************************************************************************" << std::endl;
      }
    }

    static void sumup(std::istream &resultStream, 
		      size_t queryNo, 
		      SumupValues &sumupValues,
		      std::unordered_set<size_t> &gt,
		      const size_t resultDataSize,
		      std::string &keyValue,
		      bool recall,
		      double farthestDistance)
    {
      std::string line;
      size_t lineNo = 0;
      while (getline(resultStream, line)) {
	lineNo++;
	size_t resultNo = 0;
	if (line.size() != 0 && line.at(0) == '#') {
	  std::vector<std::string> tf;
	  NGT::Common::tokenize(line, tf, "=");
	  if (tf.size() >= 1 && tf[0] == "# Query No.") {
	    if (tf.size() >= 2 && (size_t)NGT::Common::strtol(tf[1]) == queryNo) {
	      size_t relevantCount = 0;
	      size_t dataCount = 0;
	      std::string epsilon;
	      std::string expansion;  
	      double queryTime = 0.0;
	      size_t distanceCount = 0;
	      size_t visitCount = 0;
	      double totalDistance = 0.0;
	      std::unordered_set<size_t> searchedIDs;
	      while (getline(resultStream, line)) {
		lineNo++;
		if (line.size() != 0 && line.at(0) == '#') {
		  std::vector<std::string> gtf;
		  NGT::Common::tokenize(line, gtf, "=");
		  if (gtf.size() >= 2 && (gtf[0] == "# Epsilon" || gtf[0] == "# Factor")) {
		    epsilon = gtf[1];
		  } else if (gtf.size() >= 2 && gtf[0] == "# Result expansion") {
		    expansion = gtf[1];
		  } else if (gtf.size() >= 2 && gtf[0] == "# Query Time (msec)") {
		    queryTime = NGT::Common::strtod(gtf[1]);
		  } else if (gtf.size() >= 2 && gtf[0] == "# Distance Computation") {
		    distanceCount = NGT::Common::strtol(gtf[1]);
		  } else if (gtf.size() >= 2 && gtf[0] == "# Visit Count") {
		    visitCount = NGT::Common::strtol(gtf[1]);
		  } else if (gtf.size() >= 1 && gtf[0] == "# End of Query") {
		    return;
		  } else if (gtf.size() >= 1 && gtf[0] == "# End of Search") {
		    resultNo++;
		    if (recall == false && resultDataSize != dataCount) {
		      std::cerr << "Warning! ****************************************************************************" << std::endl;
		      std::cerr << "  Use $resultDataSize instead of $dataCount as the result size to compute accuracy. " <<  std::endl;
		      std::cerr << "    # of the actual search resultant objects=" << dataCount << std::endl;
		      std::cerr << "    the specified # of search objects or # of the ground truth data=" << resultDataSize << std::endl;
		      std::cerr << "    Line No.=" << lineNo << " Query No.=" << queryNo << " Result No.=" << resultNo << std::endl;
		      std::cerr << "Warning! ****************************************************************************" << std::endl;
		    }
		    double accuracy = (double)relevantCount / (double)resultDataSize;
		    double key;
		    if (epsilon != "") {
		      key = NGT::Common::strtod(epsilon);
		      keyValue = "Factor (Epsilon)";
		    } else if (expansion != "") {
		      key = NGT::Common::strtod(expansion);
		      keyValue = "Expansion";
		    } else {
		      std::stringstream msg;
		      msg << "check: inner error! " << epsilon;
		      std::cerr << "Cannot find epsilon.";
		      NGTThrowException(msg);
		    }
		    {
		      auto di = sumupValues.totalAccuracy.find(key);
		      if (di != sumupValues.totalAccuracy.end()) {
			(*di).second += accuracy;
		      } else {
			sumupValues.totalAccuracy.insert(std::make_pair(key, accuracy));
		      }
		    }
		    {
		      auto di = sumupValues.totalTime.find(key);
		      if (di != sumupValues.totalTime.end()) {
			(*di).second += queryTime;
		      } else {
			sumupValues.totalTime.insert(std::make_pair(key, queryTime));
		      }
		    }
		    {
		      auto di = sumupValues.totalDistanceCount.find(key);
		      if (di != sumupValues.totalDistanceCount.end()) {
			(*di).second += distanceCount;
		      } else {
			sumupValues.totalDistanceCount.insert(std::make_pair(key, distanceCount));
		      }
		    }
		    {
		      auto di = sumupValues.totalVisitCount.find(key);
		      if (di != sumupValues.totalVisitCount.end()) {
			(*di).second += visitCount;
		      } else {
			sumupValues.totalVisitCount.insert(std::make_pair(key, visitCount));
		      }
		    }
		    {
		      auto di = sumupValues.totalCount.find(key);
		      if (di != sumupValues.totalCount.end()) {
			(*di).second ++;
		      } else {
			sumupValues.totalCount.insert(std::make_pair(key, 1));
		      }
		    }
		    if (sumupValues.resultIsAvailable) {
		      SumupValues::Result result;
		      result.queryNo = queryNo;
		      result.key = key;
		      result.accuracy = accuracy;
		      result.time = queryTime;
		      result.distanceCount = distanceCount;
		      result.visitCount = visitCount;
		      result.meanDistance = totalDistance / (double)resultDataSize;
		      for (auto i = gt.begin(); i != gt.end(); ++i) {
			if (searchedIDs.find(*i) == searchedIDs.end()) {
			  result.unsearchedIDs.push_back(*i);
			} else {
			  result.searchedIDs.push_back(*i);
			}
		      }
		      sumupValues.results.push_back(result);
		      searchedIDs.clear();
		    }
		    totalDistance = 0.0;
		    relevantCount = 0;
		    dataCount = 0;
		  } 
		  continue;
		} 
		std::vector<std::string> result;      
		NGT::Common::tokenize(line, result, " \t");
		if (result.size() < 3) {
		  std::cerr << "result format is wrong. " << std::endl;
		  abort();
		}
		size_t rank = NGT::Common::strtol(result[0]);
		size_t id = NGT::Common::strtol(result[1]);
		double distance = NGT::Common::strtod(result[2]);
		totalDistance += distance;
		if (gt.count(id) != 0) {
		  relevantCount++;
		  if (sumupValues.resultIsAvailable) {
		    searchedIDs.insert(id);
		  }
		} else {
		  if (farthestDistance > 0.0 && distance <= farthestDistance) {
		    relevantCount++;
		    if (distance < farthestDistance) {
		    }
		  }
		}
		dataCount++;
		if (rank != dataCount) {
		  std::cerr << "inner error! $rank $dataCount !!" << std::endl;;
		  abort();
		}
	      } 
	    } else { 
	      std::cerr << "Fatal error! : Cannot find query No. " << queryNo << std::endl;
	      abort();
	    } 
	  } 
	} 
      } 
    }

    static void exploreEpsilonForAccuracy(NGT::Index &index, std::istream &queries, std::istream &gtStream, 
					  Command::SearchParameter &sp, std::pair<float, float> accuracyRange, double margin) 
    {
      double fromUnder = 0.0;
      double fromOver = 1.0;
      double toUnder = 0.0;
      double toOver = 1.0;
      float fromUnderEpsilon = -0.9;
      float fromOverEpsilon = -0.9;
      float toUnderEpsilon = -0.9;
      float toOverEpsilon = -0.9;

      float accuracyRangeFrom = accuracyRange.first;
      float accuracyRangeTo = accuracyRange.second;

      double range = accuracyRangeTo - accuracyRangeFrom;

      std::vector<MeasuredValue> acc;

      {
	float startEpsilon = -0.6;
	float epsilonStep = 0.02;
	size_t count;
	for (count = 0;; count++) {
	  float epsilon = round((startEpsilon + epsilonStep * count) * 100.0F) / 100.0F; 
	  if (epsilon > 0.25F) {
	    std::stringstream msg;
	    msg << "exploreEpsilonForAccuracy:" << std::endl;
	    msg << "Error!! Epsilon (lower bound) is too large. " << epsilon << "," << startEpsilon << "," << epsilonStep << "," << count;
	    NGTThrowException(msg);
	  }
	  acc.clear();
	  sp.beginOfEpsilon = sp.endOfEpsilon = fromOverEpsilon = epsilon;
	  queries.clear();
	  queries.seekg(0, std::ios_base::beg);
	  search(index, queries, gtStream, sp, acc);
	  if (acc[0].meanAccuracy >= accuracyRangeFrom) {
	    break;
	  }
	}
	if (fromOverEpsilon == startEpsilon) {
	  std::stringstream msg;
	  msg << "exploreEpsilonForAccuracy:" << std::endl;
	  msg << "Error! startEpsilon should be reduced for the specified range.";
	  NGTThrowException(msg);
	}
	fromOver = acc[0].meanAccuracy;

	if (fromOver < accuracyRangeTo) {
	  startEpsilon = fromOverEpsilon;
	  for (count = 0;; count++) {
	    float epsilon = round((startEpsilon + epsilonStep * count) * 100.0F) / 100.0F; 
	    sp.beginOfEpsilon = sp.endOfEpsilon = toOverEpsilon = epsilon;
	    if (epsilon > 0.25F) {
	      std::stringstream msg;
	      msg << "exploreEpsilonForAccuracy:" << std::endl;
	      msg << "Error!! Epsilon (upper bound) is too large. " << epsilon << "," << startEpsilon << "," << epsilonStep << "," << count;
	      NGTThrowException(msg);
	    }
	    acc.clear();
	    queries.clear();
	    queries.seekg(0, std::ios_base::beg);
	    search(index, queries, gtStream, sp, acc);
	    epsilon += epsilonStep;
	    if (acc[0].meanAccuracy >= accuracyRangeTo) {
	      break;
	    }
	  }
	  toOver = acc[0].meanAccuracy;
	} else {
	  toOver = fromOver;
	  toOverEpsilon = fromOverEpsilon;
	}
	fromUnderEpsilon = fromOverEpsilon - epsilonStep;
      }
      sp.beginOfEpsilon = sp.endOfEpsilon = fromUnderEpsilon;
      while (true) {
	acc.clear();
	queries.clear();
	queries.seekg(0, std::ios_base::beg);
	search(index, queries, gtStream, sp, acc);
	if (acc[0].meanAccuracy >= fromUnder && acc[0].meanAccuracy <= accuracyRangeFrom) {
	  fromUnder = acc[0].meanAccuracy;
	  fromUnderEpsilon = acc[0].keyValue;
	}
	if (acc[0].meanAccuracy <= fromOver && acc[0].meanAccuracy > accuracyRangeFrom) {
	  fromOver = acc[0].meanAccuracy;
	  fromOverEpsilon = acc[0].keyValue;
	}
	if (acc[0].meanAccuracy <= toOver && acc[0].meanAccuracy > accuracyRangeTo) {
	  toOver = acc[0].meanAccuracy;
	  toOverEpsilon = acc[0].keyValue;
	}
	if (acc[0].meanAccuracy >= toUnder && acc[0].meanAccuracy <= accuracyRangeTo) {
	  toUnder = acc[0].meanAccuracy;
	  toUnderEpsilon = acc[0].keyValue;
	}

	if (fromUnder < accuracyRangeFrom - range * margin) {
	  if ((fromUnderEpsilon + fromOverEpsilon) / 2.0 == sp.beginOfEpsilon) {
	    std::stringstream msg;
	    msg << "exploreEpsilonForAccuracy:" << std::endl;
	    msg << "Error!! Not found proper under epsilon for margin=" << margin << " and the number of queries." << std::endl;
	    msg << "        Should increase margin or the number of queries to get the proper epsilon. ";
	    NGTThrowException(msg);
	  } else {
	    sp.beginOfEpsilon = sp.endOfEpsilon = (fromUnderEpsilon + fromOverEpsilon) / 2.0;
	  }
	} else if (toOver > accuracyRangeTo + range * margin) {
	  if ((toUnderEpsilon + toOverEpsilon) / 2.0 == sp.beginOfEpsilon) {
	    std::stringstream msg;
	    msg << "exploreEpsilonForAccuracy:" << std::endl;
	    msg << "Error!! Not found proper over epsilon for margin=" << margin << " and the number of queries." << std::endl;
	    msg << "        Should increase margin or the number of queries to get the proper epsilon. ";
	    NGTThrowException(msg);
	  } else {
	    sp.beginOfEpsilon = sp.endOfEpsilon = (toUnderEpsilon + toOverEpsilon) / 2.0;
	  }
	} else {
	  if (fromUnderEpsilon == toOverEpsilon) {
	    std::stringstream msg;
	    msg << "exploreEpsilonForAccuracy:" << std::endl;
	    msg << "Error!! From and to epsilons are the same. Cannot continue.";
	    NGTThrowException(msg);
	  }
	  sp.beginOfEpsilon = fromUnderEpsilon;
	  sp.endOfEpsilon = toOverEpsilon;
	  return;
	}
      }
      std::stringstream msg;
      msg << "Something wrong!";
      NGTThrowException(msg);
    }

    MeasuredValue measure(std::istream &queries, std::istream &gtStream, Command::SearchParameter &searchParameter, std::pair<float, float> accuracyRange, double margin) {

      exploreEpsilonForAccuracy(index, queries, gtStream, searchParameter, accuracyRange, margin);
    
      std::stringstream resultStream;
      queries.clear();
      queries.seekg(0, std::ios_base::beg);
      NGT::Command::search(index, searchParameter, queries, resultStream);
      gtStream.clear();
      gtStream.seekg(0, std::ios_base::beg);
      resultStream.clear();
      resultStream.seekg(0, std::ios_base::beg);
      std::string type;
      size_t actualResultSize = 0;
      std::vector<MeasuredValue> accuracies = evaluate(gtStream, resultStream, type, actualResultSize);
      size_t size;
      double distanceCount = 0, visitCount = 0, time = 0;
      calculateMeanValues(accuracies, accuracyRange.first, accuracyRange.second, size, distanceCount, visitCount, time);
      if (distanceCount == 0) {
	std::stringstream msg;
	msg << "measureDistance: Error! Distance count is zero.";
	NGTThrowException(msg);
      }
      MeasuredValue v;
      v.meanVisitCount = visitCount;
      v.meanDistanceCount = distanceCount;
      v.meanTime = time;
      return v;
    }

    std::pair<size_t, double> adjustBaseSearchEdgeSize(std::stringstream &queries, Command::SearchParameter &searchParameter, std::stringstream &gtStream, std::pair<float, float> accuracyRange, float marginInit = 0.2, size_t prevBase = 0) {
      searchParameter.edgeSize = -2;
      size_t minimumBase = 4;
      size_t minimumStep = 2;
      size_t baseStartInit = 1;
      while (prevBase != 0) {
	prevBase >>= 1;
	baseStartInit <<= 1;
      }
      baseStartInit >>= 2;
      baseStartInit = baseStartInit < minimumBase ? minimumBase : baseStartInit;
      while(true) {
	try {
	  float margin = marginInit;
	  size_t baseStart = baseStartInit;
	  double minTime = DBL_MAX;
	  size_t minBase = 0;
	  std::map<size_t, double> times;
	  std::cerr << "adjustBaseSearchEdgeSize: explore for the margin " << margin << ", " << baseStart << "..." << std::endl;
	  for (size_t baseStep = 16; baseStep != 1; baseStep /= 2) {
	    double prevTime = DBL_MAX;
	    for (size_t base = baseStart; ; base += baseStep) {
	      if (base > 1000) {
		std::stringstream msg;
		msg << "base is too large! " << base;
		NGTThrowException(msg);
	      }
	      searchParameter.step = 10;
	      NGT::GraphIndex &graphIndex = static_cast<GraphIndex&>(index.getIndex());
	      NeighborhoodGraph::Property &prop = graphIndex.getGraphProperty();
	      prop.dynamicEdgeSizeBase = base;
	      double time;
	      if (times.count(base) == 0) {
		for (;;) {
		  try {
		    auto values = measure(queries, gtStream, searchParameter, accuracyRange, margin);
		    time = values.meanTime;
		    break;
		  } catch(NGT::Exception &err) {
		    if (err.getMessage().find("Error!! Epsilon") != std::string::npos &&
			err.getMessage().find("is too large") != std::string::npos) {
		      std::cerr << "Warning: Cannot adjust the base edge size." << err.what() << std::endl;
		      std::cerr << "Try again with the next base" << std::endl;
		      NGTThrowException("**Retry**"); 
		    }
		    if (margin > 0.4) {
		      std::cerr << "Warning: Cannot adjust the base even for the widest margin " << margin << ". " << err.what();
		      NGTThrowException("**Retry**"); 
		    } else {
		      std::cerr << "Warning: Cannot adjust the base edge size for margin " << margin << ". " << err.what() << std::endl;
		      std::cerr << "Try again for the next margin." << std::endl;
		      margin += 0.05;
		    }
		  }
		}
		times.insert(std::make_pair(base, time));
	      } else {
		time = times.at(base);
	      }
	      if (prevTime <= time) {
		if (baseStep == minimumStep) {
		  return std::make_pair(minBase, minTime);
		} else {
		  baseStart = static_cast<int>(minBase) - static_cast<int>(baseStep) < static_cast<int>(baseStart) ? baseStart : minBase - baseStep;
		  break;
		}
	      }
	      prevTime = time;
	      if (time < minTime) {
		minTime = time;
		minBase = base;
	      }
	    }
	  }
	} catch(NGT::Exception &err) {
	  if (err.getMessage().find("**Retry**") != std::string::npos) {
	    baseStartInit += minimumStep;
	  } else {
	    throw err;
	  }
	}
      }
    }

    size_t adjustBaseSearchEdgeSize(std::pair<float, float> accuracyRange, size_t querySize, double epsilon, float margin = 0.2) {
      std::cerr << "adjustBaseSearchEdgeSize: Extract queries for GT..." << std::endl;
      std::stringstream queries;
      extractQueries(querySize, queries);

      std::cerr << "adjustBaseSearchEdgeSize: create GT..." << std::endl;
      Command::SearchParameter searchParameter;
      searchParameter.edgeSize = -1;
      std::stringstream gtStream;
      createGroundTruth(index, epsilon, searchParameter, queries, gtStream);

      auto base = adjustBaseSearchEdgeSize(queries, searchParameter, gtStream, accuracyRange, margin);
      return base.first;
    }


    std::pair<size_t, double> adjustRateSearchEdgeSize(std::stringstream &queries, Command::SearchParameter &searchParameter, std::stringstream &gtStream, std::pair<float, float> accuracyRange, float marginInit = 0.2, size_t prevRate = 0) {
      searchParameter.edgeSize = -2;
      size_t minimumRate = 2;
      size_t minimumStep = 4;
      size_t rateStartInit = 1;
      while (prevRate != 0) {
	prevRate >>= 1;
	rateStartInit <<= 1;
      }
      rateStartInit >>= 2;
      rateStartInit = rateStartInit < minimumRate ? minimumRate : rateStartInit;
      while (true) {
	try {
	  float margin = marginInit;
	  size_t rateStart = rateStartInit;
	  double minTime = DBL_MAX;
	  size_t minRate = 0;
	  std::map<size_t, double> times;
	  std::cerr << "adjustRateSearchEdgeSize: explore for the margin " << margin << ", " << rateStart << "..." << std::endl;
	  for (size_t rateStep = 16; rateStep != 1; rateStep /= 2) {
	    double prevTime = DBL_MAX;
	    for (size_t rate = rateStart; rate < 2000; rate += rateStep) {
	      if (rate > 1000) {
		std::stringstream msg;
		msg << "rate is too large! " << rate;
		NGTThrowException(msg);
	      }
	      searchParameter.step = 10;
	      NGT::GraphIndex &graphIndex = static_cast<GraphIndex&>(index.getIndex());
	      NeighborhoodGraph::Property &prop = graphIndex.getGraphProperty();
	      prop.dynamicEdgeSizeRate = rate;
	      double time;
	      if (times.count(rate) == 0) {
		for (;;) {
		  try {
		    auto values = measure(queries, gtStream, searchParameter, accuracyRange, margin);
		    time = values.meanTime;
		    break;
		  } catch(NGT::Exception &err) {
		    if (err.getMessage().find("Error!! Epsilon") != std::string::npos &&
			err.getMessage().find("is too large") != std::string::npos) {
		      std::cerr << "Warning: Cannot adjust the rate of edge size." << err.what() << std::endl;
		      std::cerr << "Try again with the next rate" << std::endl;
		      NGTThrowException("**Retry**");
		    }
		    if (margin > 0.4) {
		      std::cerr << "Error: Cannot adjust the rate even for the widest margin " << margin << ". " << err.what();
		      NGTThrowException("**Retry**"); 
		    } else {
		      std::cerr << "Warning: Cannot adjust the rate of edge size for margin " << margin << ". " << err.what() << std::endl;
		      std::cerr << "Try again for the next margin." << std::endl;
		      margin += 0.05;
		    }
		  }
		}
		times.insert(std::make_pair(rate, time));
	      } else {
		time = times.at(rate);
	      }
	      if (prevTime <= time) {
		if (rateStep == minimumStep) {
		  return std::make_pair(minRate, minTime);
		} else {
		  rateStart = static_cast<int>(minRate) - static_cast<int>(rateStep) < static_cast<int>(rateStart) ? rateStart : minRate - rateStep;
		  break;
		}
	      }
	      prevTime = time;
	      if (time < minTime) {
		minTime = time;
		minRate = rate;
	      }
	    }
	  }
	} catch(NGT::Exception &err) {
	  if (err.getMessage().find("**Retry**") != std::string::npos) {
	    rateStartInit += minimumStep;
	  } else {
	    throw err;
	  }
	}
      }
    }



    std::pair<size_t, size_t> adjustSearchEdgeSize(std::pair<float, float> baseAccuracyRange, std::pair<float, float> rateAccuracyRange, size_t querySize, double epsilon, float margin = 0.2) {


      std::stringstream queries;
      std::stringstream gtStream;

      Command::SearchParameter searchParameter;
      searchParameter.edgeSize = -1;
      NGT::GraphIndex &graphIndex = static_cast<GraphIndex&>(index.getIndex());
      NeighborhoodGraph::Property &prop = graphIndex.getGraphProperty();
      searchParameter.size = nOfResults;
      redirector.begin();
      try {
	std::cerr << "adjustSearchEdgeSize: Extract queries for GT..." << std::endl;
	extractQueries(querySize, queries);
	std::cerr << "adjustSearchEdgeSize: create GT..." << std::endl;
	createGroundTruth(index, epsilon, searchParameter, queries, gtStream);
      } catch (NGT::Exception &err) {
	std::cerr << "adjustSearchEdgeSize: Error!! Cannot adjust. " << err.what() << std::endl;
	redirector.end();
	return std::pair<size_t, size_t>(0, 0);
      }
      redirector.end();

      auto prevBase = std::pair<size_t, double>(0, 0);
      auto prevRate = std::pair<size_t, double>(0, 0);
      auto base = std::pair<size_t, double>(0, 0);
      auto rate = std::pair<size_t, double>(20, 0);

      std::map<std::pair<size_t, size_t>, double> history;
      redirector.begin();
      for(;;) {
	try {
	  prop.dynamicEdgeSizeRate = rate.first;
	  prevBase = base;
	  base = adjustBaseSearchEdgeSize(queries, searchParameter, gtStream, baseAccuracyRange, margin, prevBase.first);
	  std::cerr << "adjustRateSearchEdgeSize: Base: base=" << prevBase.first << "->" << base.first << ",rate=" << prevRate.first << "->" << rate.first << std::endl;
	  if (prevBase.first == base.first) {
	    break;
	  }
	  prop.dynamicEdgeSizeBase = base.first;
	  prevRate = rate;
	  rate = adjustRateSearchEdgeSize(queries, searchParameter, gtStream, rateAccuracyRange, margin, prevRate.first);
	  std::cerr << "adjustRateSearchEdgeSize: Rate base=" << prevBase.first << "->" << base.first << ",rate=" << prevRate.first << "->" << rate.first << std::endl;
	  if (prevRate.first == rate.first) {
	    break;
	  }
	  if (history.count(std::make_pair(base.first, rate.first)) != 0) {
	    std::cerr << "adjustRateSearchEdgeSize: Warning! Found an infinite loop." << std::endl;
	    double minTime = rate.second;
	    std::pair<size_t, size_t> min = std::make_pair(base.first, rate.first);
	    for (auto i = history.begin(); i != history.end(); ++i) {
	      double dc = (*i).second;
	      if (dc < minTime) {
		minTime = dc;
		min = (*i).first;
	      }
	    }
	    return min;
	  }
	  // store parameters here to prioritize high accuracy
	  history.insert(std::make_pair(std::make_pair(base.first, rate.first), rate.second));
	} catch (NGT::Exception &err) {
	  std::cerr << "adjustRateSearchEdgeSize: Error!! Cannot adjust. " << err.what() << std::endl;
	  redirector.end();
	  return std::pair<size_t, size_t>(0, 0);
	}
      }
      redirector.end();
      return std::make_pair(base.first, rate.first);
    }

    static void adjustSearchEdgeSize(Args &args)
    {
      const std::string usage = "Usage: ngt adjust-edge-size [-m margin] [-e epsilon-for-ground-truth] [-q #-of-queries] [-n #-of-results] index";

      std::string indexName;
      try {
	indexName = args.get("#1");
      } catch (...) {
	std::cerr << "ngt: Error: DB is not specified" << std::endl;
	std::cerr << usage << std::endl;
	return;
      }

      std::pair<float, float> baseAccuracyRange = std::pair<float, float>(0.30, 0.50);
      std::pair<float, float> rateAccuracyRange = std::pair<float, float>(0.80, 0.90);

      std::string opt = args.getString("A", "");
      if (opt.size() != 0) {
	std::vector<std::string> tokens;
	NGT::Common::tokenize(opt, tokens, ":");
	if (tokens.size() >= 1) { baseAccuracyRange.first = NGT::Common::strtod(tokens[0]); }
	if (tokens.size() >= 2) { baseAccuracyRange.second = NGT::Common::strtod(tokens[1]); }
	if (tokens.size() >= 3) { rateAccuracyRange.first = NGT::Common::strtod(tokens[2]); }
	if (tokens.size() >= 4) { rateAccuracyRange.second = NGT::Common::strtod(tokens[3]); }
      }

      double margin = args.getf("m", 0.2);
      double epsilon = args.getf("e", 0.1);
      size_t querySize = args.getl("q", 100);
      size_t nOfResults = args.getl("n", 10);

      std::cerr << "adjustRateSearchEdgeSize: range= " << baseAccuracyRange.first << "-" << baseAccuracyRange.second 
	   << "," << rateAccuracyRange.first << "-" << rateAccuracyRange.second << std::endl;
      std::cerr << "adjustRateSearchEdgeSize: # of queries=" << querySize << std::endl;

      NGT::Index	index(indexName);

      Optimizer		optimizer(index, nOfResults);
      try {
	auto v = optimizer.adjustSearchEdgeSize(baseAccuracyRange, rateAccuracyRange, querySize, epsilon, margin);
	NGT::GraphIndex &graphIndex = static_cast<GraphIndex&>(index.getIndex());
	NeighborhoodGraph::Property &prop = graphIndex.getGraphProperty();
	if (v.first > 0) {
	  prop.dynamicEdgeSizeBase = v.first;
	}
	if (v.second > 0) {
	  prop.dynamicEdgeSizeRate = v.second;
	}
	if (prop.dynamicEdgeSizeRate > 0 && prop.dynamicEdgeSizeBase > 0) {
	  graphIndex.saveProperty(indexName);
	}
      } catch (NGT::Exception &err) {
	std::cerr << "adjustRateSearchEdgeSize: Error!! Cannot adjust. " << err.what() << std::endl;
	return;
      }
    }


    void outputObject(std::ostream &os, std::vector<float> &v, NGT::Property &prop) {
      switch (prop.objectType) {
      case NGT::ObjectSpace::ObjectType::Uint8:
	{
	  for (auto i = v.begin(); i != v.end(); ++i) {
	    int d = *i;
	    os << d;
	    if (i + 1 != v.end()) {
	      os << "\t";
	    }
	  }
	  os << std::endl;
	}
	break;
      default:
      case NGT::ObjectSpace::ObjectType::Float:
	{
	  for (auto i = v.begin(); i != v.end(); ++i) {
	    os << *i;
	    if (i + 1 != v.end()) {
	      os << "\t";
	    }
	  }
	  os << std::endl;
	}
	break;
      }
    }

    void outputObjects(std::vector<std::vector<float>> &vs, std::ostream &os) {
      NGT::Property prop;
      index.getProperty(prop);

      for (auto i = vs.begin(); i != vs.end(); ++i) {
	outputObject(os, *i, prop);
      }
    }

    std::vector<float> extractObject(size_t id, NGT::Property &prop) {
      std::vector<float> v;
      switch (prop.objectType) {
      case NGT::ObjectSpace::ObjectType::Uint8:
	{
	  auto *obj = static_cast<uint8_t*>(index.getObjectSpace().getObject(id));
	  for (int i = 0; i < prop.dimension; i++) {
	    int d = *obj++;
	    v.push_back(d);
	  }
	}
	break;
      default:
      case NGT::ObjectSpace::ObjectType::Float:
	{
	  auto *obj = static_cast<float*>(index.getObjectSpace().getObject(id));
	  for (int i = 0; i < prop.dimension; i++) {
	    float d = *obj++;
	    v.push_back(d);
	  }
	}
	break;
      }
      return v;
    }

    std::vector<float> meanObject(size_t id1, size_t id2, NGT::Property &prop) {
      std::vector<float> v;
      switch (prop.objectType) {
      case NGT::ObjectSpace::ObjectType::Uint8:
	{
	  auto *obj1 = static_cast<uint8_t*>(index.getObjectSpace().getObject(id1));
	  auto *obj2 = static_cast<uint8_t*>(index.getObjectSpace().getObject(id2));
	  for (int i = 0; i < prop.dimension; i++) {
	    int d = (*obj1++ + *obj2++) / 2;
	    v.push_back(d);
	  }
	}
	break;
      default:
      case NGT::ObjectSpace::ObjectType::Float:
	{
	  auto *obj1 = static_cast<float*>(index.getObjectSpace().getObject(id1));
	  auto *obj2 = static_cast<float*>(index.getObjectSpace().getObject(id2));
	  for (int i = 0; i < prop.dimension; i++) {
	    float d = (*obj1++ + *obj2++) / 2.0F;
	    v.push_back(d);
	  }
	}
	break;
      }
      return v;
    }

    void extractQueries(std::vector<std::vector<float>> &queries, std::ostream &os) {
      NGT::Property prop;
      index.getProperty(prop);

      for (auto i = queries.begin(); i != queries.end(); ++i) {
	outputObject(os, *i, prop);
      }
    }

    void extractQueries(size_t nqueries, std::ostream &os, bool similarObject = false) {

      std::vector<std::vector<float>> queries;
      extractQueries(nqueries, queries, similarObject);

      extractQueries(queries, os);
    }

    void extractAndRemoveRandomQueries(size_t nqueries, std::vector<std::vector<float>> &queries) {
      NGT::Property prop;
      index.getProperty(prop);
      size_t repositorySize = index.getObjectRepositorySize();
      NGT::ObjectRepository &objectRepository = index.getObjectSpace().getRepository();
      
      queries.clear();

      size_t emptyCount = 0;
      while (nqueries > queries.size()) {
	double random = ((double)rand() + 1.0) / ((double)RAND_MAX + 2.0);
	size_t idx = floor(repositorySize * random) + 1;
	if (objectRepository.isEmpty(idx)) {
	  emptyCount++;
	  if (emptyCount >= 1000) {
	    std::stringstream msg;
	    msg << "Too small amount of objects. " << repositorySize << ":" << nqueries;
	    NGTThrowException(msg);
	  }
	  continue;
	}
	queries.push_back(extractObject(idx, prop));
	objectRepository.erase(idx);
      }
    }

    void extractQueries(size_t nqueries, std::vector<std::vector<float>> &queries, bool similarObject = false) {

      NGT::Property prop;
      index.getProperty(prop);

      size_t osize = index.getObjectRepositorySize();
      size_t interval = osize / nqueries;
      size_t count = 0;
      for (size_t id1 = 1; id1 < osize && count < nqueries; id1 += interval, count++) {
	size_t oft = 0;
	while (index.getObjectSpace().getRepository().isEmpty(id1 + oft)) {
	  oft++;
	  if (id1 + oft >= osize) {
	    std::stringstream msg;
	    msg << "Too many empty entries to extract. Object repository size=" << osize << " " << id1 << ":" << oft;
	    NGTThrowException(msg);
	  }
	}
	if (similarObject) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  NGT::Object *query = index.getObjectSpace().allocateObject(*index.getObjectSpace().getRepository().get(id1 + oft));
#else
	  NGT::Object *query = index.getObjectSpace().getRepository().get(id1 + oft);
#endif
	  NGT::SearchContainer sc(*query);
	  NGT::ObjectDistances results;
	  sc.setResults(&results);
	  sc.setSize(nOfResults);
	  index.search(sc);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  index.getObjectSpace().deleteObject(query);
#endif
	  if (results.size() < 2) {
	    std::stringstream msg;
	    msg << "Cannot get even two results for queries.";
	    NGTThrowException(msg);
	  }
	  size_t id2 = 1;
	  for (size_t i = 1; i < results.size(); i++) {
	    if (results[i].distance > 0.0) {
	      id2 = results[i].id;
	      break;
	    }
	  }
	  queries.push_back(meanObject(id1 + oft, id2, prop));
	} else {
	  size_t id2 = id1 + oft + 1;
	  while (index.getObjectSpace().getRepository().isEmpty(id2)) {
	    id2++;
	    if (id2 >= osize) {
	      std::stringstream msg;
	      msg << "Too many empty entries to extract.";
	      NGTThrowException(msg);
	    }
	  }
	  queries.push_back(meanObject(id1 + oft, id2, prop));
	}
      }
      assert(count == nqueries);
    
    }


    static void
      extractQueries(Args &args)
    {
      const std::string usage = "Usage: ngt eval-query -n #-of-queries index";

      std::string indexName;
      try {
	indexName = args.get("#1");
      } catch (...) {
	std::cerr << "ngt: Error: DB is not specified" << std::endl;
	std::cerr << usage << std::endl;
	return;
      }
      size_t nqueries = args.getl("n", 1000);

      NGT::Index	index(indexName);
      NGT::Optimizer	optimizer(index);
      optimizer.extractQueries(nqueries, std::cout);

    }

    static void createGroundTruth(NGT::Index &index, double epsilon, Command::SearchParameter &searchParameter, std::stringstream &queries, std::stringstream &gtStream){
      queries.clear();
      queries.seekg(0, std::ios_base::beg);
      searchParameter.outputMode = 'e';
      searchParameter.beginOfEpsilon = searchParameter.endOfEpsilon = epsilon;
      NGT::Command::search(index, searchParameter, queries, gtStream);
    }

    static int 
      calculateMeanValues(std::vector<MeasuredValue> &accuracies, double accuracyRangeFrom, double accuracyRangeTo, 
			  size_t &size, double &meanDistanceCount, double &meanVisitCount, double &meanTime) {
      int stat = 0;
      size = 0;
      if (accuracies.front().meanAccuracy > accuracyRangeFrom) {
	stat = 0x1;
      }
      if (accuracies.back().meanAccuracy < accuracyRangeTo) {
	stat |= 0x2;
      }
      if (stat != 0) {
	return stat;
      }
      std::vector<MeasuredValue> acc;
      acc = accuracies;
      for (auto start = acc.rbegin(); start != acc.rend(); ++start) {
	if ((*start).meanAccuracy <= accuracyRangeFrom) {
	  ++start;
	  acc.erase(acc.begin(), start.base());
	  break;
	}
      }
      for (auto end = acc.begin(); end != acc.end(); ++end) {
	if ((*end).meanAccuracy >= accuracyRangeTo) {
	  end++;
	  acc.erase(end, acc.end());
	  break;
	}
      }
      std::vector<std::pair<double, double>> distance;
      std::vector<std::pair<double, double>> visit;
      std::vector<std::pair<double, double>> time;
      for (auto i = acc.begin(); i != acc.end(); ++i) {
#ifdef NGT_LOG_BASED_OPTIMIZATION
	if ((*i).meanDistanceCount > 0.0) {
	  (*i).meanDistanceCount = log10((*i).meanDistanceCount);
	}
	if ((*i).meanVisitCount > 0.0) {
	  (*i).meanVisitCount = log10((*i).meanVisitCount);
	}
#endif
	distance.push_back(std::make_pair((*i).meanDistanceCount, (*i).meanAccuracy));
	visit.push_back(std::make_pair((*i).meanVisitCount, (*i).meanAccuracy));
	time.push_back(std::make_pair((*i).meanTime, (*i).meanAccuracy));
      }
      {
	size_t last = distance.size() - 1;
	double xfrom = (distance[1].second * distance[0].first - distance[0].second * distance[1].first + 
			accuracyRangeFrom * (distance[1].first - distance[0].first)) / 
	  (distance[1].second - distance[0].second);
	double xto = (distance[last].second * distance[last - 1].first - distance[last - 1].second * distance[last].first + 
		      accuracyRangeTo * (distance[last].first - distance[last - 1].first)) / 
	  (distance[last].second - distance[last - 1].second);
	distance[0].first = xfrom;
	distance[0].second = accuracyRangeFrom;
	distance[last].first = xto;
	distance[last].second = accuracyRangeTo;
	double area = 0.0;
	for (size_t i = 0; i < distance.size() - 1; ++i) {
	  area += ((distance[i].first + distance[i + 1].first) * (distance[i + 1].second - distance[i].second)) / 2.0;
	}
	meanDistanceCount = area / (distance[last].second - distance[0].second);
      }
      {
	size_t last = visit.size() - 1;
	double xfrom = (visit[1].second * visit[0].first - visit[0].second * visit[1].first + 
			accuracyRangeFrom * (visit[1].first - visit[0].first)) / 
	  (visit[1].second - visit[0].second);
	double xto = (visit[last].second * visit[last - 1].first - visit[last - 1].second * visit[last].first + 
		      accuracyRangeTo * (visit[last].first - visit[last - 1].first)) / 
	  (visit[last].second - visit[last - 1].second);
	visit[0].first = xfrom;
	visit[0].second = accuracyRangeFrom;
	visit[last].first = xto;
	visit[last].second = accuracyRangeTo;
	double area = 0.0;
	for (size_t i = 0; i < visit.size() - 1; ++i) {
	  area += ((visit[i].first + visit[i + 1].first) * (visit[i + 1].second - visit[i].second)) / 2.0;
	}
	meanVisitCount = area / (visit[last].second - visit[0].second);
      }
      {
	size_t last = time.size() - 1;
	double xfrom = (time[1].second * time[0].first - time[0].second * time[1].first + 
			accuracyRangeFrom * (time[1].first - time[0].first)) / 
	  (time[1].second - time[0].second);
	double xto = (time[last].second * time[last - 1].first - time[last - 1].second * time[last].first + 
		      accuracyRangeTo * (time[last].first - time[last - 1].first)) / 
	  (time[last].second - time[last - 1].second);
	time[0].first = xfrom;
	time[0].second = accuracyRangeFrom;
	time[last].first = xto;
	time[last].second = accuracyRangeTo;
	double area = 0.0;
	for (size_t i = 0; i < time.size() - 1; ++i) {
	  area += ((time[i].first + time[i + 1].first) * (time[i + 1].second - time[i].second)) / 2.0;
	}
	meanTime = area / (time[last].second - time[0].second);
      }
      assert(distance.size() == time.size());
      size = distance.size();
      return 0;
    }

    static void evaluate(Args &args)
    {
      const std::string usage = "Usage: ngt eval [-n number-of-results] [-m mode(r=recall)] [-g ground-truth-size] [-o output-mode] ground-truth search-result\n"
	"   Make a ground truth list (linear search): \n"
	"       ngt search -i s -n 20 -o e index query.list > ground-truth.list";

      std::string gtFile, resultFile;
      try {
	gtFile = args.get("#1");
      } catch (...) {
	std::cerr << "ground truth is not specified." << std::endl;
	std::cerr << usage << std::endl;
	return;
      }
      try {
	resultFile = args.get("#2");
      } catch (...) {
	std::cerr << "result file is not specified." << std::endl;
	std::cerr << usage << std::endl;
	return;
      }

      size_t resultSize = args.getl("n", 0);
      if (resultSize != 0) {
	std::cerr << "The specified number of results=" << resultSize << std::endl;
      }

      size_t groundTruthSize = args.getl("g", 0);

      bool recall = false;
      if (args.getChar("m", '-') == 'r') {
	std::cerr << "Recall" << std::endl;
	recall = true;
      }
      char omode = args.getChar("o", '-');
    
      std::ifstream	resultStream(resultFile);
      if (!resultStream) {
	std::cerr << "Cannot open the specified target file. " << resultFile << std::endl;
	std::cerr << usage << std::endl;
	return;
      }

      std::ifstream	gtStream(gtFile);
      if (!gtStream) {
	std::cerr << "Cannot open the specified GT file. " << gtFile << std::endl;
	std::cerr << usage << std::endl;
	return;
      }

      std::string type;
      size_t actualResultSize = 0;
      std::vector<MeasuredValue> accuracies =
	evaluate(gtStream, resultStream, type, actualResultSize, resultSize, groundTruthSize, recall);

      std::cout << "# # of evaluated resultant objects per query=" << actualResultSize << std::endl;
      if (recall) {
	std::cout << "# " << type << "\t# of Queries\tRecall\t";
      } else {
	std::cout << "# " << type << "\t# of Queries\tPrecision\t";
      }
      if (omode == 'd') {
	std::cout << "# of computations\t# of visted nodes" << std::endl;
	for (auto it = accuracies.begin(); it != accuracies.end(); ++it) {
	  std::cout << (*it).keyValue << "\t" << (*it).totalCount << "\t" << (*it).meanAccuracy << "\t" 
	       << (*it).meanDistanceCount << "\t" << (*it).meanVisitCount << std::endl;
	}
      } else {
	std::cout << "Time(msec)\t# of computations\t# of visted nodes" << std::endl;
	for (auto it = accuracies.begin(); it != accuracies.end(); ++it) {
	  std::cout << (*it).keyValue << "\t" << (*it).totalCount << "\t" << (*it).meanAccuracy << "\t" << (*it).meanTime << "\t" 
	       << (*it).meanDistanceCount << "\t" << (*it).meanVisitCount << std::endl;
	}
      }

    }

    void generatePseudoGroundTruth(size_t nOfQueries, float &maxEpsilon, std::stringstream &queryStream, std::stringstream &gtStream)
    {
      std::vector<std::vector<float>> queries;
      extractQueries(nOfQueries, queries);
      generatePseudoGroundTruth(queries, maxEpsilon, queryStream, gtStream);
    }

    void generatePseudoGroundTruth(std::vector<std::vector<float>> &queries, float &maxEpsilon, std::stringstream &queryStream, std::stringstream &gtStream)
    {
      size_t nOfQueries = queries.size();
      maxEpsilon = 0.0;
      {
	std::vector<NGT::Object *> queryObjects;
	for (auto i = queries.begin(); i != queries.end(); ++i) {
	  queryObjects.push_back(index.allocateObject(*i));
	}

	int identityCount = 0;
	std::vector<NGT::Distance> lastDistances(nOfQueries);
	double time = 0.0;
	double step = 0.02;
	for (float e = 0.0; e < 10.0; e += step) {
	  size_t idx;
	  bool identity = true;
	  NGT::Timer timer;
	  for (idx = 0; idx < queryObjects.size(); idx++) {
	    NGT::SearchContainer sc(*queryObjects[idx]);
	    NGT::ObjectDistances results;
	    sc.setResults(&results);
	    sc.setSize(nOfResults);
	    sc.setEpsilon(e);
	    timer.restart();
	    index.search(sc);
	    timer.stop();
	    NGT::Distance d = results.back().distance;
	    if (d != lastDistances[idx]) {
	      identity = false;
	    }
	    lastDistances[idx] = d;
	  }
	  if (e == 0.0) {
	    time = timer.time;
	  }
	  if (timer.time > time * 40.0) { 
	    maxEpsilon = e;
	    break;
	  }
	  if (identity) {
	    identityCount++;
	    step *= 1.2;
	    if (identityCount > 5) { 
	      maxEpsilon = e;
	      break;
	    }
	  } else {
	    identityCount = 0;
	  }
	}      

	for (auto i = queryObjects.begin(); i != queryObjects.end(); ++i) {
	  index.deleteObject(*i);
	}

      }

      {
	// generate (pseudo) ground truth data
	NGT::Command::SearchParameter searchParameter;
	searchParameter.size = nOfResults;
	searchParameter.outputMode = 'e';
	searchParameter.edgeSize = 0;	// get the best accuracy by using all edges
	//searchParameter.indexType = 's'; // linear search
	extractQueries(queries, queryStream);
	NGT::Optimizer::createGroundTruth(index, maxEpsilon, searchParameter, queryStream, gtStream);
      }
    }

    static std::vector<std::pair<float, double>> 
      generateAccuracyTable(NGT::Index &index, size_t nOfResults = 50, size_t querySize = 100) {

      NGT::Property prop;
      index.getProperty(prop);
      if (prop.edgeSizeForSearch != 0 && prop.edgeSizeForSearch != -2) {
	std::stringstream msg;
	msg << "Optimizer::generateAccuracyTable: edgeSizeForSearch is invalid to call generateAccuracyTable, because accuracy 1.0 cannot be achieved with the setting. edgeSizeForSearch=" << prop.edgeSizeForSearch << ".";
	NGTThrowException(msg);
      }

      NGT::Optimizer optimizer(index, nOfResults);

      float maxEpsilon = 0.0;
      std::stringstream queryStream;
      std::stringstream gtStream;

      optimizer.generatePseudoGroundTruth(querySize, maxEpsilon, queryStream, gtStream);

      std::map<float, double> map;
      {
	float interval = 0.05;
	float prev = 0.0;
	std::vector<NGT::Optimizer::MeasuredValue> acc;
	float epsilon = -0.6;
	double accuracy;
	do {
	  auto pair = map.find(epsilon);
	  if (pair == map.end()) {
	    NGT::Command::SearchParameter searchParameter;
	    searchParameter.outputMode = 'e';
	    searchParameter.beginOfEpsilon = searchParameter.endOfEpsilon = epsilon;
	    queryStream.clear();
	    queryStream.seekg(0, std::ios_base::beg);
	    NGT::Optimizer::search(index, queryStream, gtStream, searchParameter, acc);
	    if (acc.size() == 0) {
	      NGTThrowException("Fatal error! Cannot get any accuracy value.");
	    }
	    accuracy = acc[0].meanAccuracy;
	    map.insert(std::make_pair(epsilon, accuracy));
	  } else {
	    accuracy = (*pair).second;
	  }
	  if (prev != 0.0) {
	    if (accuracy - prev < 0.02) {
	      interval *= 2.0;
	    } else if (accuracy - prev > 0.05 && interval > 0.0001) {
	      
	      epsilon -= interval;
	      interval /= 2.0;
	      accuracy = prev;
	    }
	  }
	  prev = accuracy;
	  epsilon += interval;
	  if (accuracy > 0.98 && epsilon > maxEpsilon) {
	    break;
	  }
	} while (accuracy < 1.0);
      }

      std::vector<std::pair<float, double>> epsilonAccuracyMap;
      std::pair<float, double> prev(0.0, -1.0);
      for (auto i = map.begin(); i != map.end(); ++i) {
	if (fabs((*i).first - prev.first) <= FLT_EPSILON) {
	  continue;
	}
	if ((*i).second - prev.second < DBL_EPSILON) {
	  continue;
	}
	epsilonAccuracyMap.push_back(*i);
	if ((*i).second >= 1.0) {
	  break;
	}
	prev = *i;
      }

      return epsilonAccuracyMap;
    }

    NGT::Index &index;
    size_t nOfResults;
    StdOstreamRedirector redirector;
  };
}; // NGT


