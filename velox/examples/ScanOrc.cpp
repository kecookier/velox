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
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/orc/reader/OrcReader.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;

// A temporary program that reads from ORC file and prints its content
// Used to compare the ORC data read by DWRFReader against apache-orc repo.
// Usage: velox_example_scan_orc {orc_file_path}
int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  if (argc < 2) {
    return 1;
  }

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory:
  filesystems::registerLocalFileSystem();
  orc::registerOrcReaderFactory();
  facebook::velox::memory::MemoryManager::initialize({});
  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();

  std::string filePath{argv[1]};
  dwio::common::ReaderOptions readerOpts{pool.get()};
  // To make DwrfReader reads ORC file, setFileFormat to FileFormat::ORC
  readerOpts.setFileFormat(FileFormat::ORC);
  auto reader = dwio::common::getReaderFactory(FileFormat::ORC)
                    ->createReader(
                        std::make_unique<BufferedInput>(
                            std::make_shared<LocalReadFile>(filePath),
                            readerOpts.memoryPool()),
                        readerOpts);
  auto schema = reader->rowType();
  std::cout << "schema: " << schema->toString() << std::endl;

  VectorPtr batch;
  RowReaderOptions rowReaderOptions;
  auto rowReader = reader->createRowReader(rowReaderOptions);
  while (rowReader->next(500, batch)) {
    auto rowVector = batch->as<RowVector>();
    for (vector_size_t i = 0; i < rowVector->size(); ++i) {
      std::cout << rowVector->toString(i) << std::endl;
    }
  }

  // readerOption里设置 tableSchema， 然后再读取测试。结果就会错误。
  // decimal类型在orc里是如何保存的？ 用字符串保存吗？
  // orc的fileSchema如何生成的？ dwrfReader生成的吗?

  std::vector<std::string> names;
  names.reserve(schema.size());
  std::vector<TypePtr> types;
  types.reserve(schema.size());

  for (auto i = 0; i < schema.size(); ++i) {
    if (name == "duration") {
      auot type = schema[i]->type();
      dynamic_cast<>

    } else {
      names.push_back(schema[i]->name());
      types.push_back(schema[i]->type());
    }
  }

  for (auto& handle : split_->bucketConversion->bucketColumnHandles) {
    VELOX_CHECK(handle->columnType() == HiveColumnHandle::ColumnType::kRegular);
    if (subfields_.erase(handle->name()) > 0) {
      rebuildScanSpec = true;
    }
    auto index = readerOutputType_->getChildIdxIfExists(handle->name());
    if (!index.has_value()) {
      if (names.empty()) {
        names = readerOutputType_->names();
        types = readerOutputType_->children();
      }
      index = names.size();
      names.push_back(handle->name());
      types.push_back(hiveTableHandle_->dataColumns()->findChild(handle->name()));
      rebuildScanSpec = true;
    }
    bucketChannels.push_back(*index);
  }
  if (!names.empty()) {
    readerOutputType_ = ROW(std::move(names), std::move(types));
  }

  return 0;
}
