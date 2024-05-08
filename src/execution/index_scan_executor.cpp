//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_oid_t table_id = plan_->table_oid_;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id);

  index_oid_t index_id = plan_->index_oid_;
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(index_id);
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  std::vector<Value> values{};
  values.push_back(plan_->pred_key_->val_);
  Tuple key_tuple(values, &index_info_->key_schema_);
  htable_->ScanKey(key_tuple, &rids_, exec_ctx_->GetTransaction());
  index_num_ = 0;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &table_heap = table_info_->table_;
  while (index_num_ < rids_.size()) {
    *rid = rids_[index_num_];
    *tuple = table_heap->GetTuple(*rid).second;
    auto tuple_meta = table_heap->GetTupleMeta(*rid);
    if (!tuple_meta.is_deleted_) {
      bool filter_result = true;
      if (plan_->filter_predicate_ != nullptr) {
        auto &filter_expr = plan_->filter_predicate_;
        Value value = filter_expr->Evaluate(tuple, GetOutputSchema());
        filter_result = !value.IsNull() && value.GetAs<bool>();
      }
      if (filter_result) {
        index_num_++;
        return true;
      }
    }
    index_num_++;
  }
  return false;
}


}  // namespace bustub
