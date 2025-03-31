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

#include <fmt/format.h>
#include <numeric>
#include <utility>

#include "folly/io/Cursor.h"
#include "velox/dwio/common/BufferedInput.h"

DEFINE_bool(wsVRLoad, false, "Use WS VRead API to load");

using ::facebook::velox::common::Region;

namespace facebook::velox::dwio::common {

static_assert(std::is_move_constructible<BufferedInput>());

namespace {
void copyIOBufToMemory(folly::IOBuf&& iobuf, folly::Range<char*> allocated) {
  folly::io::Cursor cursor(&iobuf);
  VELOX_CHECK_EQ(cursor.totalLength(), allocated.size(), "length mismatch.");
  cursor.pull(allocated.data(), allocated.size());
}
} // namespace

uint64_t BufferedInput::nextFetchSize() const {
  return std::accumulate(
      regions_.cbegin(), regions_.cend(), 0L, [](uint64_t a, const Region& b) {
        return a + b.length;
      });
}

// enqueue...load模式: 把regions_里所有的 region 从文件加载到 buffers_。
// 首先清空 buffers_ 和 offsets_，然后对 regions_
// 排序和合并，得到准确的大小，然后为每个region申请内存，把region的数据加载到buffers_中。
void BufferedInput::load(const LogType logType) {
  // no regions to load
  if (regions_.size() == 0) {
    return;
  }

  offsets_.clear();
  buffers_.clear();
  allocPool_->clear();

  sortRegions();
  mergeRegions();

  // After sorting and merging we have the accurate sizes
  offsets_.reserve(regions_.size());
  buffers_.reserve(regions_.size());

  // 如果文件系统支持向量化读取，一次并行读取所有regions，否则顺序读取。读取之前为每个regions申请内存，以folly::Range的形式保存到
  // buffers_ 里。
  // 并行读取: 调用 FileReadInputStream::vread 把每个Region的数据加载到
  // folly::IOBuf， 然后再拷贝到内存
  // 加载完成之后，清空queue regions_
  if (useVRead()) {
    LOG(INFO) << "use vread";
    // Now we have all buffers and regions, load it in parallel
    std::vector<folly::IOBuf> iobufs(regions_.size());
    input_->vread(regions_, {iobufs.data(), iobufs.size()}, logType);
    for (size_t i = 0; i < regions_.size(); ++i) {
      copyIOBufToMemory(std::move(iobufs[i]), allocate(regions_[i]));
    }
  } else {
    for (const auto& region : regions_) {
      readToBuffer(region.offset, allocate(region), logType);
    }
  }

  // clear the loaded regions.
  regions_.clear();
}

// 调用 FileReadInputStream::read() 把单个Region的数据加载到folly::Range里
void BufferedInput::readToBuffer(
    uint64_t offset,
    folly::Range<char*> allocated,
    const LogType logType) {
  uint64_t usec = 0;
  {
    MicrosecondTimer timer(&usec);
    input_->read(allocated.data(), allocated.size(), offset, logType);
  }
  if (auto* stats = input_->getStats()) {
    stats->read().increment(allocated.size());
    stats->queryThreadIoLatency().increment(usec);
  }
}

// 如果使用 enqueue...load 模式时，enqueue记录用请求的region
std::unique_ptr<SeekableInputStream> BufferedInput::enqueue(
    Region region,
    const dwio::common::StreamIdentifier* /*sid*/) {
  if (region.length == 0) {
    return std::make_unique<SeekableArrayInputStream>(
        static_cast<const char*>(nullptr), 0);
  }

  // If the region is already in buffer - such as metadata.
  auto ret = readBuffer(region.offset, region.length);
  if (ret != nullptr) {
    return ret;
  }

  // push to region pool and give the caller the callback
  regions_.push_back(region);
  return std::make_unique<SeekableArrayInputStream>(
      // Save "i", the position in which this region was enqueued. This will
      // help faster lookup using enqueuedToBufferOffset_ later.
      [region, this, i = regions_.size() - 1]() {
        auto result = readInternal(region.offset, region.length, i);
        VELOX_CHECK(
            std::get<1>(result) != MAX_UINT64,
            "Fail to read region offset={} length={}",
            region.offset,
            region.length);
        return result;
      });
}

bool BufferedInput::useVRead() const {
  // Use value explicitly set by the user if any, otherwise use the GFLAG
  // We want to update this on every use for now because during the onboarding
  // to wsVRLoad=true we may change the value of this GFLAG programatically from
  // a config update so we can rollback fast from config without the need of a
  // deployment
  return wsVRLoad_.value_or(FLAGS_wsVRLoad);
}

// Sort regions and enqueuedToOffset in the same way
// 借助 enqueuedToBufferOffset_ 做sort和merge
void BufferedInput::sortRegions() {
  auto& r = regions_;
  auto& e = enqueuedToBufferOffset_;

  e.resize(r.size());
  std::iota(e.begin(), e.end(), 0);

  if (std::is_sorted(r.cbegin(), r.cend())) {
    return;
  }

  // Sort indices from low to high regions
  // "e" will contain the positions to which each region should be sorted to
  std::sort(
      e.begin(), e.end(), [&](size_t a, size_t b) { return r[a] < r[b]; });

  // Now actually sort. This way we sorted and saved the mapping of the sort
  std::vector<Region> regions;
  regions.reserve(r.size());
  for (auto i : e) {
    regions.push_back(r[i]);
  }
  std::swap(r, regions);
}

/*
把数组展开成hashtable的形式，更容易理解merge过程。
Merge完成之后，regions_ 保留merge之后的region， enqueuedToBufferOffset_
标记那几个region被合并了。

regions:
<0, {6,3}>
<1, {24,3}>
<2, {3,3}>
<3, {0,3}>
<4, {29,3}>

after sort:
enqueuedToBufferOffset_:
<0, 3>
<1, 2>
<2, 0>
<3, 1>
<4, 4>

regions:
<3, {0,3}>
<2, {3,3}>
<0, {6,3}>
<1, {24,3}>
<4, {29,3}>
=>
<0, {0,3}>
<1, {3,3}>
<2, {6,3}>
<3, {24,3}>
<4, {29,3}>

when merge:
te[e[0]] = 0  <3, 0>

ia = 0,ib = 1; te[e[1]] = 0  <3, 0> <2, 0>
ia = 0,ib = 2; te[e[2]] = 0  <3, 0> <2, 0> <0, 0>
ia = 0,ib = 3; r[1] = r[3], te[e[3]] = 1  <3, 0> <2, 0> <0, 0> <1,1>
ia = 1,ib = 4; r[2] = r[4], te[e[4]] = 2 <3, 0> <2, 0> <0, 0> <1,1> <4, 2>
te[e[2]] = te[0] = 0

regions:
<0, {0,9}>
<1, {24,3}>
<2, {29,3}>

swap(te, enqueuedToBufferOffset_)
<0, 0>
<1, 1>
<2, 0>
<3, 0>
<4, 2>
*/
void BufferedInput::mergeRegions() {
  auto& r = regions_;
  VELOX_CHECK(!r.empty(), "Assumes that there's at least one region");
  auto& e = enqueuedToBufferOffset_;
  // We want to map here where each region ended in the final merged regions
  // vector.
  // For example, if this is the regions vector: {{6, 3}, {24, 3}, {3, 3}, {0,
  // 3}, {29, 3}} After sorting, "e" would look like this: [3,2,0,1,4]. Because
  // region in position number 3 ended up in position 0 and so on.
  // For a maxMergeDistance of 1, "te" will look like: [0,1,0,0,2], because
  // original regions 3, 2 and 0 were merged into a larger region, now in
  // position 0. The original region 1, became region 1, and original region 4
  // became region 2
  std::vector<size_t> te(e.size());
  te[e[0]] = 0;

  size_t ia = 0;
  VELOX_CHECK_GT(r[ia].length, 0, "invalid region");
  for (size_t ib = 1; ib < r.size(); ++ib) {
    VELOX_CHECK_GT(r[ib].length, 0, "invalid region");
    if (!tryMerge(r[ia], r[ib])) {
      r[++ia] = r[ib];
    }
    te[e[ib]] = ia;
  }
  // After merging, remove what's left.
  r.resize(ia + 1);
  std::swap(e, te);
}

// 向量化读只有完全重复的region可以合并
// 合并算法：例如合并a和b，已知b在a后边，gap表示b的起始位置和,a的末尾的差值。extension
// 表示合并后a要扩展多少长度。
//  如果 gap>0，有空隙； gap==0：没空隙； gap<0：b的起始地址在a里
//  如果 extension<0，b在a里； extension==0，a和b一样；
bool BufferedInput::tryMerge(Region& first, const Region& second) {
  VELOX_CHECK_GE(second.offset, first.offset, "regions should be sorted.");
  const int64_t gap = second.offset - first.offset - first.length;

  // Duplicate regions (extension==0) is the only case allowed to merge for
  // useVRead()
  const int64_t extension = gap + second.length;
  if (useVRead()) {
    return extension == 0;
  }

  // compare with 0 since it's comparison in different types
  if (gap < 0 || gap <= maxMergeDistance_) {
    // the second region is inside first one if extension is negative
    if (extension > 0) {
      first.length += extension;
      if ((input_->getStats() != nullptr) && gap > 0) {
        input_->getStats()->incRawOverreadBytes(gap);
      }
    }
    return true;
  }
  return false;
}

std::unique_ptr<SeekableInputStream> BufferedInput::readBuffer(
    uint64_t offset,
    uint64_t length) const {
  const auto result = readInternal(offset, length);
  const auto size = std::get<1>(result);
  if (size == MAX_UINT64) {
    return {};
  }
  return std::make_unique<SeekableArrayInputStream>(std::get<0>(result), size);
}

// 尝试从 buffers_ 里读取需求数据[offset, offset+length]。
// offsets_数组是有序的，二分查找找到offset所在的buffer。如果buffer完全包含所需数据，返回buffer的起始地址和长度。
std::tuple<const char*, uint64_t> BufferedInput::readInternal(
    uint64_t offset,
    uint64_t length,
    std::optional<size_t> i) const {
  // return dummy one for zero length stream
  if (length == 0) {
    return std::make_tuple(nullptr, 0);
  }

  std::optional<size_t> index;
  if (i.has_value()) {
    const auto vi = i.value();
    // There's a possibility that our user enqueued, then tried to read before
    // calling load(). In that case, enqueuedToBufferOffset_ will be empty or
    // have the values from a previous load. So I want to make sure that he ends
    // up in a valid offset, and that this offset is <= offset. Otherwise we
    // just go for the binary search.
    // 调用 enqueue之后，没有load，而是 read。 这时候 buffers 保留的是上次
    // enqueue...load 组合的数据。
    if (vi < enqueuedToBufferOffset_.size() &&
        enqueuedToBufferOffset_[vi] < offsets_.size() &&
        offsets_[enqueuedToBufferOffset_[vi]] <= offset) {
      index = enqueuedToBufferOffset_[vi];
    }
  }

  if (!index.has_value()) {
    // Binary search to get the first fileOffset for which: offset < fileOffset
    const auto it =
        std::upper_bound(offsets_.cbegin(), offsets_.cend(), offset);
    // If the first element was already greater than the target offset we don't
    // have it.
    if (it != offsets_.cbegin()) {
      index = std::distance(offsets_.cbegin(), it) - 1;
    }
  }

  if (index.has_value()) {
    const uint64_t bufferOffset = offsets_[index.value()];
    const auto& buffer = buffers_[index.value()];
    if (bufferOffset + buffer.size() >= offset + length) {
      VELOX_CHECK_LE(bufferOffset, offset, "Invalid offset for readInternal");
      VELOX_CHECK_LE(
          (offset - bufferOffset) + length,
          buffer.size(),
          "Invalid readOffset for read Internal ",
          fmt::format(
              "{} {} {} {}", offset, bufferOffset, length, buffer.size()));
      return std::make_tuple(buffer.data() + (offset - bufferOffset), length);
    }
  }

  return std::make_tuple(nullptr, MAX_UINT64);
}

} // namespace facebook::velox::dwio::common
