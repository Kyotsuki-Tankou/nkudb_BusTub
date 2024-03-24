//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include <iostream>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  // If there are no evictable frames, return false
  if (curr_size_ == 0) {
    return false;
  }
  if (node_store_.empty())
  {
    return false;
  }
  // Find the frame with the largest backward k-distance
  frame_id_t evict_id = -1;
  size_t max_backward_k_distance = 0;
  for (const auto &node : node_store_) {
    if (!node.second.is_evictable_) {
      continue;
    }

    size_t backward_k_distance = current_timestamp_ - node.second.history_[node.second.history_.size() - 1];    
    // std::cout<<"Node:"<<node.first<<" dist:"<<backward_k_distance<<"\n";
    if (backward_k_distance > max_backward_k_distance) {
      max_backward_k_distance = backward_k_distance;
      evict_id = node.first;
    }
  }

  // If multiple frames have the same largest backward k-distance, use LRU to choose victim
  for (const auto &node : node_store_) {
    if (!node.second.is_evictable_) {
      continue;
    }
    size_t backward_k_distance = current_timestamp_ - node.second.history_[node.second.history_.size() - 1];
    if (backward_k_distance == max_backward_k_distance && node.first != evict_id &&
        node.second.history_.front() < node_store_[evict_id].history_.front()) {
      evict_id = node.first;
    }
  }
  Dbg();
  // Evict the chosen frame
  *frame_id = evict_id;
  node_store_.erase(evict_id);
  curr_size_--;
  
  return true;
}

// auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
//     {
//         auto candidate = node_store_.begin();
//         int candidate_dist = 0;
//         bool candidate_dist_inf = false;
//         bool found = false;
//         for (auto iter = node_store_.begin(); iter != node_store_.end(); iter++)
//         {
//             if (iter->second.is_evictable_ == false)
//             {
//                 continue;
//             }
//             else
//             {
//                 found = true;
//             }
//             int curr_dist = 0;
//             if (iter->second.history_.size() < this->k_)
//             {
//                 if (candidate_dist_inf == false)
//                 {
//                     candidate = iter;
//                     candidate_dist_inf = true;
//                 }
//                 else
//                 {
//                     if (*(iter->second.history_.begin()) < *(candidate->second.history_.begin()))
//                     {
//                         candidate = iter;
//                     }
//                 }
//             }
//             else
//             {
//                 if (candidate_dist_inf == true)
//                 {
//                     continue;
//                 }
//                 auto kth_access = iter->second.history_.begin();
//                 for (long unsigned int i = 0; i < this->k_ - 1; i += 1)
//                 {
//                     kth_access++;
//                 }
//                 curr_dist = *(iter->second.history_.begin()) - *kth_access;
//                 if (curr_dist > candidate_dist)
//                 {
//                     candidate = iter;
//                     candidate_dist = curr_dist;
//                 }
//             }
//         }
//         if (found == true)
//         {
//             *frame_id=candidate->second.fid_;
//             this->node_store_.erase(candidate->second.fid_);
//             Dbg();
//             return true;
//         }
//         else
//         {
//           Dbg();
//             return false;
//         }
//     }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_)  // valid?
  {
    throw std::invalid_argument("Invalid frame_id");
  }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end())  // Didn't find, create a new one
  {
    LRUKNode tmp;
    tmp.fid_ = frame_id;
    tmp.k_ = k_;
    tmp.history_.assign(k_, -11451419);
    tmp.is_evictable_ = true;
    node_store_.insert({frame_id, tmp});
    it = node_store_.find(frame_id);
  }
  // move right for space of the recorded timestamp
  for (size_t i = k_ - 1; i > 0; i--) {
    it->second.history_[i] = it->second.history_[i - 1];
  }
  it->second.history_[0] = current_timestamp_++;
  // update size
  if (!it->second.is_evictable_) {
    it->second.is_evictable_ = true;
    curr_size_++;
  }
  Dbg();
  // This function should be called after a page is pinned in the BufferPoolManager.
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    it->second.is_evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else if (it->second.is_evictable_) {
      curr_size_--;
    }
  }
  Dbg();
  return;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    // Clear all access history associated with a frame.
    it->second.history_.assign(k_, 0);
    // This method should be called only when a page is deleted in the BufferPoolManager.
    it->second.is_evictable_ = false;
    curr_size_--;
  }
  Dbg();
  return;
}
auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  long long res=0;
  for (const auto &node : node_store_) {
    if (!node.second.is_evictable_) {
      continue;
    }
    res++;
  }
  return res;
}

}  // namespace bustub
