// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // extendible_htable_header_page.cpp
// //
// // Identification: src/storage/page/extendible_htable_header_page.cpp
// //
// // Copyright (c) 2015-2023, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"
#include <iostream>

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  // Initialize directory_page_ids_ array
  for (uint32_t i = 0; i < (1u << max_depth_); i++) {
    directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  std::cout << ((max_depth_ == 0) ? 0 : hash >> (32 - max_depth_)) << "\n";
  return (max_depth_ == 0) ? 0 : hash >> (32 - max_depth_); ;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  // if (directory_idx >= (1u << max_depth_)) {
  //   throw std::out_of_range("Out of range");
  // }
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // if (directory_idx >= (1u << max_depth_)) {
  //   throw std::out_of_range("Out of range");
  // }
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1u << max_depth_; }

}  // namespace bustub