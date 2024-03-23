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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::unique_lock<std::mutex> lock(latch_);
  // If there are no evictable frames, return false
  if (curr_size_ == 0) {
    return false;
  }

  // Find the frame with the largest backward k-distance
  frame_id_t evict_id = -1;
  size_t max_backward_k_distance = 0;
  for (const auto& node : node_store_) {
    if (!node.second.is_evictable_) {
      continue;
    }
    size_t backward_k_distance = current_timestamp_ - node.second.history_.back();
    if (backward_k_distance > max_backward_k_distance) {
      max_backward_k_distance = backward_k_distance;
      evict_id = node.first;
    }
  }

  // If multiple frames have the same largest backward k-distance, use LRU to choose victim
  for (const auto& node : node_store_) {
    if (!node.second.is_evictable_ || node.second.history_.size() < k_) {
      continue;
    }
    size_t backward_k_distance = current_timestamp_ - node.second.history_[k_ - 1];
    if (backward_k_distance == max_backward_k_distance && node.first != evict_id && node.second.history_.front() < node_store_[evict_id].history_.front()) {
      evict_id = node.first;
    }
  }

  // Evict the chosen frame
  *frame_id = evict_id;
  node_store_.erase(evict_id);
  curr_size_--;
  return true;
 }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type)
{
  std::unique_lock<std::mutex> lock(latch_);
  if(frame_id>=replacer_size_)//valid?
  {
    throw std::invalid_argument("Invalid frame_id");
  }
  auto it=node_store_.find(frame_id);
  if(it==node_store_.end())//Didn't find, create a new one
  {
    LRUKNode tmp;
    tmp.fid_=frame_id;
    tmp.k_=k_;
    tmp.history_.assign(k_,0);
    tmp.is_evictable_=true;
    node_store_.insert({frame_id,tmp});
    it=node_store_.find(frame_id);
  }
  //move left for space of the recorded timestamp
  for(size_t i=k_-1;i>0;i--)
  {
    it->second.history_[i]=it->second.history_[i-1];
  }
  it->second.history_[0]=current_timestamp_++;
  //update size
  if(!it->second.is_evictable_)
  {
    it->second.is_evictable_=true;
    curr_size_++;
  }
}
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  auto it=node_store_.find(frame_id);
  if(it!=node_store_.end())
  {
    it->second.is_evictable_=set_evictable;
    curr_size_+=set_evictable?1:-1;
  }
  return;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);

  auto it=node_store_.find(frame_id);
  if(it!=node_store_.end()||it->second.is_evictable_)
  {
    return;
  }
  node_store_.erase(it);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { 
  std::unique_lock<std::mutex> lock(latch_);
  return curr_size_; }

}  // namespace bustub
