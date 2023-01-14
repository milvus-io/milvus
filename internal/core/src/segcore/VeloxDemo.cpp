/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/init/Init.h>
#include <algorithm>

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;

// A temporary program that reads from ORC file and prints its content
// Used to compare the ORC data read by DWRFReader against apache-orc repo.
// Usage: velox_example_scan_orc {orc_file_path}
int
main(int argc, char** argv) {
    folly::init(&argc, &argv);

    if (argc < 2) {
        return 1;
    }

    // To be able to read local files, we need to register the local file
    // filesystem. We also need to register the dwrf reader factory:
    filesystems::registerLocalFileSystem();
    dwrf::registerDwrfReaderFactory();

    std::string filePath{argv[1]};
    ReaderOptions readerOpts;
    // To make DwrfReader reads ORC file, setFileFormat to FileFormat::ORC
    readerOpts.setFileFormat(FileFormat::ORC);
    auto reader = DwrfReader::create(
        std::make_unique<BufferedInput>(std::make_shared<LocalReadFile>(filePath), readerOpts.getMemoryPool()),
        readerOpts);

    VectorPtr batch;
    RowReaderOptions rowReaderOptions;
    auto rowReader = reader->createRowReader(rowReaderOptions);
    while (rowReader->next(500, batch)) {
        auto rowVector = batch->as<RowVector>();
        for (vector_size_t i = 0; i < rowVector->size(); ++i) {
            std::cout << rowVector->toString(i) << std::endl;
        }
    }

    return 0;
}
