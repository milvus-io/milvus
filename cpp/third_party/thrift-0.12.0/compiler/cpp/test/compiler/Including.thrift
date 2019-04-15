include "Included.thrift"

const string s = "string"

struct BStruct {
  1: Included.a_struct one_of_each
}
