#include "yaml-cpp/emitterstyle.h"
#include "yaml-cpp/eventhandler.h"
#include "yaml-cpp/yaml.h"  // IWYU pragma: keep

#include <cstdlib>
#include <fstream>
#include <iostream>

class NullEventHandler : public YAML::EventHandler {
 public:
  typedef YAML::Mark Mark;
  typedef YAML::anchor_t anchor_t;

  NullEventHandler() {}

  virtual void OnDocumentStart(const Mark&) {}
  virtual void OnDocumentEnd() {}
  virtual void OnNull(const Mark&, anchor_t) {}
  virtual void OnAlias(const Mark&, anchor_t) {}
  virtual void OnScalar(const Mark&, const std::string&, anchor_t,
                        const std::string&) {}
  virtual void OnSequenceStart(const Mark&, const std::string&, anchor_t,
                               YAML::EmitterStyle::value style) {}
  virtual void OnSequenceEnd() {}
  virtual void OnMapStart(const Mark&, const std::string&, anchor_t,
                          YAML::EmitterStyle::value style) {}
  virtual void OnMapEnd() {}
};

void run(std::istream& in) {
  YAML::Parser parser(in);
  NullEventHandler handler;
  parser.HandleNextDocument(handler);
}

void usage() { std::cerr << "Usage: read [-n N] [-c, --cache] [filename]\n"; }

std::string read_stream(std::istream& in) {
  return std::string((std::istreambuf_iterator<char>(in)),
                     std::istreambuf_iterator<char>());
}

int main(int argc, char** argv) {
  int N = 1;
  bool cache = false;
  std::string filename;
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "-n") {
      i++;
      if (i >= argc) {
        usage();
        return -1;
      }
      N = std::atoi(argv[i]);
      if (N <= 0) {
        usage();
        return -1;
      }
    } else if (arg == "-c" || arg == "--cache") {
      cache = true;
    } else {
      filename = argv[i];
      if (i + 1 != argc) {
        usage();
        return -1;
      }
    }
  }

  if (N > 1 && !cache && filename == "") {
    usage();
    return -1;
  }

  if (cache) {
    std::string input;
    if (filename != "") {
      std::ifstream in(filename);
      input = read_stream(in);
    } else {
      input = read_stream(std::cin);
    }
    std::istringstream in(input);
    for (int i = 0; i < N; i++) {
      in.seekg(std::ios_base::beg);
      run(in);
    }
  } else {
    if (filename != "") {
      std::ifstream in(filename);
      for (int i = 0; i < N; i++) {
        in.seekg(std::ios_base::beg);
        run(in);
      }
    } else {
      for (int i = 0; i < N; i++) {
        run(std::cin);
      }
    }
  }
  return 0;
}
