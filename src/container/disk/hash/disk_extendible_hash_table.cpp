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
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page=header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t val=Hash(key);
  auto index=header_page->HashToDirectoryIndex(val);
  page_id_t dir_page_id=header_page->GetDirectoryPageId(index);
  if(dir_page_id==INVALID_PAGE_ID)  return false;

  ReadPageGuard dir_guard=bpm_->FetchPageRead(dir_page_id);
  auto dir_page=dir_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_index=dir_page->HashToBucketIndex(val);
  auto bucket_id=dir_page->GetBucketPageId(bucket_index);
  if(bucket_id==INVALID_PAGE_ID)  return false;

  ReadPageGuard bucket_guard=bpm_->FetchPageRead(bucket_id);
  V res;
  auto bucket_page=bucket_guard.As<ExtendibleHTableBucketPage<K,V,KC>>();
  bool success=bucket_page->Lookup(key,res,cmp_);
  if(!success)  return false;
  result->push_back(res);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  //level1 to level2
  auto header_guard=bpm->FetchPageWrite(header_page_id_);
  auto header_page=header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t val=Hash(key);
  uint32_t dir_index=header_page->HashToDirectoryIndex(hash);
  auto dir_id=header_page->GetDirectoryPageId(dir_index);
  if(dir_id==INVALID_PAGE_ID)
  {
    auto success=InsertToNewDirectory(header_page,dir_index,val,key,value);
    return success;
  }

  //level2 to level3
  header_guard.Drop();
  WritePageGuard dir_guard=bom_->FetchPageWrite(dir_id);
  auto dir_page=directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index=dir_page->HashToBucketIndex(hash);
  auto bucket_id=dir_page->GetBucketPageId(bucket_index);
  if(bucket_id==INVALID_PAGE_ID)
  {
    auto success=InsertToNewBucket(dir_page,bucket_index,key,value);
    return success;
  }
  
  //bucket full or not
  bool success=false;
  WritePageGuard bucket_guard=bpm->FetchPageWrite(bucket_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KV>>();
  V tmp;
  if(bucket_page->Lookup(key,tmp,cmp_))  return false;
  if(!bucket_page->IsFull())
  {
    success=bucket_page->Insert(key,value,cmp_);
    return success;
  }
  while(!success&&bucket_page->IsFull())
  {
    if(dir_page->GetGlobalDepth()==dir_page->GetLocalDepth(bucket_index))
    {
      if(dir_page->GetGlobalDepth()==dir_page->GetMaxDepth())  return false;
      dir_page->IncrGlobalDepth();
    }
    page_id_t new_id;
    auto tmp_bucket_guard=bpm_->NewPageGuarded(&new_id);
    auto new_bucket_guard=tmp_bucket_guard.UpgradeWrite();
    auto new_bucket_page=new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    new_bucket_page->Init(bucket_max_size_);
    directory_page->IncrLocalDepth(bucket_index);
    auto new_localDepth=dir_page->GetLocalDepth(bucket_index);
    auto localDepthMask=dir_page->GetLocalDepthMask(bucket_index);
    auto new_bucket_id=UpdateDirectoryMapping(directory_page,bucket_index,new_id,new_localDepth,localDepthMask);
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
