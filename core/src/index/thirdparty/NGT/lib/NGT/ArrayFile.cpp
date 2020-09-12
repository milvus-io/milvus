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

#include "ArrayFile.h"
#include <iostream>
#include <assert.h>

class ItemID {
public:
  void serialize(std::ostream &os, NGT::ObjectSpace *ospace = 0) {
    os.write((char*)&value, sizeof(value));
  }
  void deserialize(std::istream &is, NGT::ObjectSpace *ospace = 0) {
    is.read((char*)&value, sizeof(value));
  }
  static size_t getSerializedDataSize() {
    return sizeof(uint64_t);
  }
  uint64_t value;
};

void
sampleForUsage() {
  {
    ArrayFile<ItemID> itemIDFile;
    itemIDFile.create("test.data", ItemID::getSerializedDataSize());
    itemIDFile.open("test.data");
    ItemID itemID;
    size_t id;

    id = 1;
    itemID.value = 4910002490100;
    itemIDFile.put(id, itemID);
    itemID.value = 0;
    itemIDFile.get(id, itemID);
    std::cerr << "value=" << itemID.value << std::endl;
    assert(itemID.value == 4910002490100);

    id = 2;
    itemID.value = 4910002490101;
    itemIDFile.put(id, itemID);
    itemID.value = 0;
    itemIDFile.get(id, itemID);
    std::cerr << "value=" << itemID.value << std::endl;
    assert(itemID.value == 4910002490101);

    itemID.value = 4910002490102;
    id = itemIDFile.insert(itemID);
    itemID.value = 0;
    itemIDFile.get(id, itemID);
    std::cerr << "value=" << itemID.value << std::endl;
    assert(itemID.value == 4910002490102);

    itemIDFile.close();
  }
  {
    ArrayFile<ItemID> itemIDFile;
    itemIDFile.create("test.data", ItemID::getSerializedDataSize());
    itemIDFile.open("test.data");
    ItemID itemID;
    size_t id;

    id = 10;
    itemIDFile.get(id, itemID);
    std::cerr << "value=" << itemID.value << std::endl;
    assert(itemID.value == 4910002490100);

    id = 20;
    itemIDFile.get(id, itemID);
    std::cerr << "value=" << itemID.value << std::endl;
    assert(itemID.value == 4910002490101);
  }

}


