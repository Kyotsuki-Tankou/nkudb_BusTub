//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  index_name_ = name;
  page_id_t page_id;
  auto tmp_guard = bpm_->NewPageGuarded(&page_id);
  auto header_guard = tmp_guard.UpgradeWrite();
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
  header_page_id_ = page_id;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t val = Hash(key);
  auto index = header_page->HashToDirectoryIndex(val);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(index);
  if (dir_page_id == INVALID_PAGE_ID) return false;

  ReadPageGuard dir_guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = dir_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_index = dir_page->HashToBucketIndex(val);
  auto bucket_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_id == INVALID_PAGE_ID) return false;

  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_id);
  V res;
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  bool success = bucket_page->Lookup(key, res, cmp_);
  if (!success) return false;
  result->push_back(res);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // level1 to level2
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t val = Hash(key);
  uint32_t dir_index = header_page->HashToDirectoryIndex(val);
  auto dir_id = header_page->GetDirectoryPageId(dir_index);
  if (int(dir_id) == int(INVALID_PAGE_ID)) {
    auto success = InsertToNewDirectory(header_page, dir_index, val, key, value);
    // std::cout<<"successIns1"<<success<<std::endl;
    return success;
  }

  // level2 to level3
  header_guard.Drop();
  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = dir_page->HashToBucketIndex(val);
  auto bucket_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_id == INVALID_PAGE_ID) {
    auto success = InsertToNewBucket(dir_page, bucket_index, key, value);
    // std::cout<<"successIns2"<<success<<std::endl;
    return success;
  }

  // bucket full or not
  bool success = false;
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  V tmp;
  if (bucket_page->Lookup(key, tmp, cmp_)) return false;
  if (!bucket_page->IsFull()) {
    success = bucket_page->Insert(key, value, cmp_);
    return success;
  }
  while (!success && bucket_page->IsFull()) {
    if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(bucket_index)) {
      if (dir_page->GetGlobalDepth() == dir_page->GetMaxDepth()) return false;
      dir_page->IncrGlobalDepth();
    }
    page_id_t new_id;
    auto tmp_bucket_guard = bpm_->NewPageGuarded(&new_id);
    auto new_bucket_guard = tmp_bucket_guard.UpgradeWrite();
    auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    dir_page->IncrLocalDepth(bucket_index);
    auto new_localDepth = dir_page->GetLocalDepth(bucket_index);
    auto localDepthMask = dir_page->GetLocalDepthMask(bucket_index);
    auto new_bucket_idx = UpdateDirectoryMapping(dir_page, bucket_index, new_id, new_localDepth, localDepthMask);
    bucket_index = new_bucket_idx; // update bucket_index

    // rehash
    page_id_t rehash_id;
    std::vector<uint32_t> remove_array;
    for (uint32_t i = 0; i < bucket_page->Size(); i++) {
      auto k = bucket_page->KeyAt(i);
      auto v = bucket_page->ValueAt(i);
      uint32_t h = Hash(k);
      auto rehash_idx = dir_page->HashToBucketIndex(h);
      rehash_id = dir_page->GetBucketPageId(rehash_idx);
      if (rehash_id == new_id) {
        new_bucket_page->Insert(k, v, cmp_);
        remove_array.push_back(i);
      }
    }
    auto flag = 0;
    for (auto &remove_id : remove_array) {
      bucket_page->RemoveAt(remove_id - flag);
      flag++;
    }
    // insert
    bucket_index = dir_page->HashToBucketIndex(val);
    rehash_id = dir_page->GetBucketPageId(bucket_index);
    if (rehash_id == new_id) {
      success = new_bucket_page->Insert(key, value, cmp_);
      if (!success && new_bucket_page->IsFull()) {
        // std::cout<<1123<<std::endl;
        // continue the loop to split the new bucket page
        bucket_guard = std::move(new_bucket_guard);
        bucket_id = new_id;
        bucket_page = new_bucket_page;
        bucket_index = new_bucket_idx;
      }
    } else {
      success = bucket_page->Insert(key, value, cmp_);
    }
  }
  return success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t dir_page_id;
  auto tmp_dir_guard = bpm_->NewPageGuarded(&dir_page_id);
  auto dir_guard = tmp_dir_guard.UpgradeWrite();
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, dir_page_id);
  auto bucket_idx = dir_page->HashToBucketIndex(hash); 
  int success=InsertToNewBucket(dir_page, bucket_idx, key, value);
  return success;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_id;
  // std::cout<<1111<<std::endl;
  auto tmp_guard = bpm_->NewPageGuarded(&bucket_id);
  auto bucket_guard = tmp_guard.UpgradeWrite();
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  // std::cout<<2222<<std::endl;
  bucket_page->Init(bucket_max_size_);
  // std::cout<<11111<<std::endl;
  directory->SetBucketPageId(bucket_idx, bucket_id);
  // std::cout<<22222<<std::endl;
  directory->SetLocalDepth(bucket_idx, 0);
  // std::cout<<3333<<std::endl;
  auto insert_success = bucket_page->Insert(key, value, cmp_);
  // std::cout<<"successNB"<<insert_success<<std::endl;
  return insert_success;
}

template <typename K, typename V, typename KC>
uint32_t DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                                   uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                                   uint32_t new_local_depth, uint32_t local_depth_mask) {
    // uint32_t old_bucket_count = 1 << (new_local_depth - 1);
    // uint32_t old_bucket_idx = local_depth_mask & new_bucket_idx;
    // uint32_t distance = 1u << new_local_depth;

    // uint32_t new_first_bucket_idx = old_bucket_idx + (new_bucket_idx >= old_bucket_count ? old_bucket_count : 0);

    // uint32_t prime_idx = new_first_bucket_idx;

    // for (uint32_t i = new_first_bucket_idx; i < directory->Size(); i += distance) {
    //     directory->SetBucketPageId(i, new_bucket_page_id);
    //     directory->SetLocalDepth(i, new_local_depth);
    //     directory->SetLocalDepth(prime_idx, new_local_depth);
    //     assert(directory->GetLocalDepth(i) <= directory->GetGlobalDepth());
    //     assert(directory->GetLocalDepth(prime_idx) <= directory->GetGlobalDepth());
    //     prime_idx += distance;
    // }
    // return new_first_bucket_idx;
    auto new_first_bucket_idx = local_depth_mask & new_bucket_idx;
    auto prime_idx = new_first_bucket_idx;
    uint32_t distance = pow(2, new_local_depth);
    new_first_bucket_idx = (new_first_bucket_idx >> (new_local_depth - 1)) == 0 ? (new_first_bucket_idx + (distance / 2))
                                                                                : (new_first_bucket_idx - (distance / 2));
    for (uint32_t i = new_first_bucket_idx; i < directory->Size(); i += distance) {
      directory->SetBucketPageId(i, new_bucket_page_id);
      directory->SetLocalDepth(i, new_local_depth);
      directory->SetLocalDepth(prime_idx, new_local_depth);
      assert(directory->GetLocalDepth(i) <= directory->GetGlobalDepth());
      assert(directory->GetLocalDepth(prime_idx) <= directory->GetGlobalDepth());
      prime_idx += distance;
    }
    return new_first_bucket_idx;
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  //level1->level2
  auto header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page=header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t val=Hash(key);
  auto dir_index=header_page->HashToDirectoryIndex(val);
  auto dir_id=header_page->GetDirectoryPageId(dir_index);
  if(int(dir_id)==INVALID_PAGE_ID)  return false;
  //level2->level3
  WritePageGuard dir_guard=bpm_->FetchPageWrite(dir_id);
  auto dir_page=dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index=dir_page->HashToBucketIndex(val);
  auto bucket_id=dir_page->GetBucketPageId(bucket_index);
  std::cout<<111<<std::endl;
  if(bucket_id==INVALID_PAGE_ID)  return false;

  //find key
  WritePageGuard bucket_guard=bpm_->FetchPageWrite(bucket_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC> >();
  bool success=bucket_page->Remove(key,cmp_);
  std::cout<<222<<std::endl;
  if(!success)  return false;

  while(bucket_page->IsEmpty())
  {
    bucket_guard.Drop();
    auto bucket_local_depth=dir_page->GetLocalDepth(bucket_index);
    // std::cout<<333<<std::endl;
    // std::cout<<bucket_local_depth<<std::endl;
    if(bucket_local_depth==0)  
    {
      while(dir_page->CanShrink())  dir_page->DecrGlobalDepth();
      break;
      }
    auto merge_index=dir_page->GetSplitImageIndex(bucket_index);
    auto merge_id=dir_page->GetBucketPageId(merge_index);
    auto merge_local_depth=dir_page->GetLocalDepth(merge_index);
    
    if(bucket_local_depth==merge_local_depth)
    {
      uint32_t new_index=std::min(bucket_index & dir_page->GetLocalDepthMask(bucket_index), merge_index);
      uint32_t dist=(1<<(bucket_local_depth-1));
      uint32_t new_local_depth=bucket_local_depth-1;
      for(uint32_t i=new_index;i<dir_page->Size();i+=dist)
      {
        dir_page->SetBucketPageId(i,merge_id);
        dir_page->SetLocalDepth(i,new_local_depth);
      }
      // dir_page->SetBucketPageId(bucket_index, 0);
      std::cout<<444<<std::endl;
      if(new_local_depth==0)  
      {
        while(dir_page->CanShrink())  dir_page->DecrGlobalDepth();
        break;
      }
      auto split_index=dir_page->GetSplitImageIndex(merge_index);
      auto split_id=dir_page->GetBucketPageId(split_index);
      WritePageGuard split_guard=bpm_->FetchPageWrite(split_id);
      if(split_id==INVALID_PAGE_ID)  {
        while(dir_page->CanShrink())  dir_page->DecrGlobalDepth();
        break;
        }
      auto flag=bucket_id;
      bucket_index=split_index;
      bucket_id=split_id;
      bucket_guard = std::move(split_guard);
      bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC> >();
      bpm_->DeletePage(flag);
    }
    else  
    {
      while(dir_page->CanShrink())  dir_page->DecrGlobalDepth();
      break;
    }
    while(dir_page->CanShrink())  dir_page->DecrGlobalDepth();
  }
  return success;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
