include "JavaTypes.thrift"

namespace java thrift.test

struct DeepCopyFoo {
  1: optional list<DeepCopyBar> l,
  2: optional set<DeepCopyBar> s,
  3: optional map<string, DeepCopyBar> m,
  4: optional list<JavaTypes.Object> li,
  5: optional set<JavaTypes.Object> si,
  6: optional map<string, JavaTypes.Object> mi,
  7: optional DeepCopyBar bar,
}

struct DeepCopyBar {
  1: optional string a,
  2: optional i32 b,
  3: optional bool c,
}
