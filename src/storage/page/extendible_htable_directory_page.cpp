//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (uint32_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; ++i) {
    local_depths_[i] = 0;
  }

  for (uint32_t i = 0; i < HTABLE_DIRECTORY_ARRAY_SIZE; ++i) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t { return hash & ((1 << global_depth_) - 1); }

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t { 
  if (bucket_idx >= (1u << global_depth_)) {
    throw std::out_of_range("Bucket index out of range");
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= (1u << global_depth_)) {
    throw std::out_of_range("Bucket index out of range");
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1)); }

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {return (1 << global_depth_) - 1;}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {return (1 << local_depths_[bucket_idx]) - 1;}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_;}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ >= max_depth_) {
    throw std::out_of_range("depth out of maximum depth");
  }
  ++global_depth_;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    throw std::out_of_range("depth already at minimum depth");
  }
  --global_depth_;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool { return global_depth_ > 0; }

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {  return 1 << global_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t { return 1 << max_depth_; }

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if ((int)bucket_idx >= (1 << global_depth_)) {
    throw std::out_of_range("bucket out of range");
  }
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if ((int)bucket_idx >= (1 << global_depth_)) {
    throw std::out_of_range("Bucket out of range");
  }
    ++local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if ((int)bucket_idx >= (1 << global_depth_)) {
    throw std::out_of_range("Bucket out of range");
  }
  if (local_depths_[bucket_idx] == 0) {
    throw std::out_of_range("already at minimum depth");
  }
  --local_depths_[bucket_idx];
}

}  // namespace bustub
