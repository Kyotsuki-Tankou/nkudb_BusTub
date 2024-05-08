//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  if (!free_list_.empty()) {
    // std::cout<<1111<<std::endl;
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();

    // Allocate new page
    *page_id = AllocatePage();

    // Reset
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    // std::cout << "New: " << frame_id << std::endl;
    // Update the page table and replacer
    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // Return a pointer to the new page
    return &pages_[frame_id];
  }
  // std::cout<<2222<<std::endl;
  // If there is no free frame, try to evict a page using the replacer
  frame_id_t victim_frame_id;
  replacer_->Evict(&victim_frame_id);
  // If the victim frame is invalid, return nullptr
  if (victim_frame_id < 0) {
    return nullptr;
  }
  // std::cout << "New: " << victim_frame_id << std::endl;
  // If the victim frame is pinned, return nullptr
  if (pages_[victim_frame_id].GetPinCount() > 0) {
    // std::cout<<3333<<std::endl;
    return nullptr;
  }

  // If the victim frame is dirty, write it back to disk
  if (pages_[victim_frame_id].IsDirty()) {
    // std::cout<<4444<<std::endl;
    // std::cout << pages_[victim_frame_id].GetPageId() << std::endl;
    // FlushPage(pages_[victim_frame_id].GetPageId());
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule(
        {true, pages_[victim_frame_id].GetData(), pages_[victim_frame_id].page_id_, std::move(promise)});
    future.get();
  }

  page_table_.erase(pages_[victim_frame_id].GetPageId());
  *page_id = AllocatePage();
  pages_[victim_frame_id].page_id_ = *page_id;
  pages_[victim_frame_id].is_dirty_ = false;
  pages_[victim_frame_id].pin_count_ = 1;
  page_table_[*page_id] = victim_frame_id;
  replacer_->RecordAccess(victim_frame_id);
  replacer_->SetEvictable(victim_frame_id, false);
  return &pages_[victim_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // std::cout << "Fetch" << std::endl;
  // First search for page_id in the buffer pool
  if (page_table_.find(page_id) != page_table_.end()) {
    // If found, return the page and update its access history
    frame_id_t frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    // std::cout << 11111 << std::endl;
    return &pages_[frame_id];
  }

  // If not found, try to find a replacement frame from the free list
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    // std::cout << 22222 << std::endl;
  } else {
    // If the free list is empty, find a replacement frame from the replacer
    replacer_->Evict(&frame_id);
    // std::cout << 33333 << std::endl;
    // std::cout << frame_id << std::endl;
    if (frame_id < 0) return nullptr;
    auto &helper = pages_[frame_id];
    if (helper.IsDirty()) {
      auto promise1 = disk_scheduler_->CreatePromise();
      auto future1 = promise1.get_future();
      disk_scheduler_->Schedule({true, helper.data_, helper.page_id_, std::move(promise1)});
      future1.get();
    }
    page_table_.erase(helper.page_id_);
    // std::cout << 33334 << std::endl;
  }
  // std::cout << 44444 << std::endl;
  // Read the page from disk by scheduling a read request with disk_manager_->ReadPage()
  std::cout << page_id << " " << frame_id << " " << pages_[frame_id].GetData() << std::endl;
  disk_scheduler_->disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  // std::cout << 55555 << std::endl;
  // Replace the old page in the frame and update the metadata of the new page
  page_table_[page_id] = frame_id;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  // Disable eviction and record the access history of the frame
  replacer_->SetEvictable(frame_id, true);
  // std::cout << 66666 << std::endl;
  replacer_->RecordAccess(frame_id);
  // std::cout << 77777 << std::endl;
  return &pages_[frame_id];
  // auto iter = page_table_.find(page_id);
  // if (iter != page_table_.end()) {
  //   auto frame_id = iter->second;
  //   replacer_->SetEvictable(frame_id, false);
  //   replacer_->RecordAccess(frame_id, access_type);
  //   pages_[iter->second].pin_count_++;
  //   return &pages_[iter->second];
  // }

  // frame_id_t replacement_frame_id;
  // if (!free_list_.empty()) {
  //   replacement_frame_id = free_list_.front();
  //   free_list_.pop_front();
  // } else {
  //   if (!replacer_->Evict(&replacement_frame_id)) {
  //     return nullptr;
  //   }
  //   auto &helper = pages_[replacement_frame_id];
  //   if (helper.IsDirty()) {
  //     auto promise1 = disk_scheduler_->CreatePromise();
  //     auto future1 = promise1.get_future();
  //     disk_scheduler_->Schedule({true, helper.data_, helper.page_id_, std::move(promise1)});
  //     future1.get();
  //   }
  //   page_table_.erase(helper.page_id_);
  // }

  // pages_[replacement_frame_id].ResetMemory();
  // // reset metadata of the page
  // pages_[replacement_frame_id].pin_count_ = 0;
  // pages_[replacement_frame_id].is_dirty_ = false;

  // auto promise2 = disk_scheduler_->CreatePromise();
  // auto future2 = promise2.get_future();
  // disk_scheduler_->Schedule({false, pages_[replacement_frame_id].GetData(), page_id, std::move(promise2)});

  // page_table_.insert(std::make_pair(page_id, replacement_frame_id));
  // replacer_->SetEvictable(replacement_frame_id, false);
  // replacer_->RecordAccess(replacement_frame_id, access_type);
  // pages_[replacement_frame_id].pin_count_++;
  // pages_[replacement_frame_id].page_id_ = page_id;

  // future2.get();
  // return pages_ + replacement_frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  // std::cout<<1110<<std::endl;
  std::scoped_lock<std::mutex> lock(latch_);
  // std::cout<<1111<<std::endl;
  // Find the frame associated with the page
  auto it = page_table_.find(page_id);
  // std::cout << "Unpin: " << it->first << " " << it->second << '\n';
  if (it == page_table_.end()) {
    // std::cout<<2222<<std::endl;
    // Page not found in page table
    return false;
  }

  // Decrement the pin count of the page
  if (pages_[it->second].GetPinCount() < 0) {
    // Pin count is already <= 0, return false
    // std::cout<<3333<<std::endl;
    return false;
  }
  pages_[it->second].pin_count_--;

  // if (!is_dirty) {
  //   // std::cout<<4444<<std::endl;
  //     pages_[it->second].is_dirty_=true;
  // }
  pages_[it->second].is_dirty_ = true;
  // If the pin count is now 0, make the frame evictable by the replacer
  if (pages_[it->second].GetPinCount() == 0) {
    // std::cout<<5555<<std::endl;
    replacer_->SetEvictable(it->second, true);
  }
  // std::cout<<6666<<std::endl;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  // std::cout<<1111<<std::endl;
  // std::cout << page_id << std::endl;
  // std::cout << pages_[it->second].GetData() << std::endl;
  // Write
  disk_scheduler_->disk_manager_->WritePage(page_id, pages_[it->second].GetData());
  pages_[it->second].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (auto &it : page_table_) {
    if (pages_[it.second].IsDirty()) {
      disk_scheduler_->disk_manager_->WritePage(it.first, pages_[it.second].GetData());
      pages_[it.second].is_dirty_ = false;
    }
  }
}

// auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
//   // std::lock_guard<std::mutex> guard(latch_);
//   // Check if the page exists in the page table
//   auto it = page_table_.find(page_id);
//   if (it == page_table_.end()) {
//     // Page doesn't exist, return true
//     return true;
//   }

//   if (pages_[it->second].GetPinCount() > 0) {
//     return false;
//   }

//   // Remove the page from the page table
//   page_table_.erase(it);
//   replacer_->Evict(&(it->second));

//   free_list_.push_back(it->second);

//   pages_[it->second].ResetMemory();
//   pages_[it->second].page_id_ = INVALID_PAGE_ID;
//   pages_[it->second].is_dirty_ = false;
//   pages_[it->second].pin_count_ = 0;

//   // Call DeallocatePage to imitate freeing the page on the disk
//   DeallocatePage(page_id);

//   return true;
// }
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  auto target_id = iter->second;
  if (pages_[target_id].pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_id);
  free_list_.push_back(target_id);
  pages_[target_id].ResetMemory();
  // reset metadata of the page
  pages_[target_id].page_id_ = INVALID_PAGE_ID;
  pages_[target_id].pin_count_ = 0;
  pages_[target_id].is_dirty_ = false;

  DeallocatePage(page_id);

  return true;
}
auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

// auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

// auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

// auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

// auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::lock_guard<std::mutex> guard(latch_);
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return BasicPageGuard(this, nullptr);
  } else {
    return BasicPageGuard(this, page);
  }
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto pg_ptr = FetchPage(page_id);
  pg_ptr->RLatch();
  return {this, pg_ptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto pg_ptr = FetchPage(page_id);
  pg_ptr->WLatch();
  return {this, pg_ptr};
}
auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto pg_ptr = NewPage(page_id);
  return {this, pg_ptr};
}
}  // namespace bustub
// namespace bustub
