//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
      plan_=plan;
      child_executor_=std::move(child_executor);
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { 
  child_executor_->Init();
  is_end_ = false;
 }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  if (is_end_) {
    return false;
  }
  table_oid_t table_id = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(table_id);
  index_array_ = catalog->GetTableIndexes(table_info_->name_);
  int32_t row_amount = 0;
  Tuple child_tuple{};
  while (child_executor_->Next(&child_tuple, rid)) {

    auto &table_heap = table_info_->table_;
    TupleMeta tuple_meta{};
    tuple_meta.is_deleted_ = true;
    tuple_meta.ts_ = 0;
    table_heap->UpdateTupleMeta(tuple_meta, *rid);

    for (auto &index_info : index_array_) {
      auto &index = index_info->index_;
      index->DeleteEntry(child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index->GetKeyAttrs()),
                         *rid, exec_ctx_->GetTransaction());
    }

    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple inserted_tuple = Tuple(values, &child_executor_->GetOutputSchema());

    TupleMeta inserted_tuple_meta{};
    inserted_tuple_meta.ts_ = 0;
    inserted_tuple_meta.is_deleted_ = false;
    auto new_rid = table_heap->InsertTuple(inserted_tuple_meta, inserted_tuple, exec_ctx_->GetLockManager(),
                                           exec_ctx_->GetTransaction(), table_info_->oid_);
    if (new_rid == std::nullopt) {
      return false;
    }

    for (auto &affected_index : index_array_) {
      affected_index->index_->InsertEntry(inserted_tuple.KeyFromTuple(table_info_->schema_, affected_index->key_schema_,
                                                                      affected_index->index_->GetKeyAttrs()),
                                          new_rid.value(), exec_ctx_->GetTransaction());
    }

    row_amount++;
  }
  auto row_value = Value(INTEGER, row_amount);
  std::vector<Value> output{};
  output.reserve(GetOutputSchema().GetColumnCount());
  output.push_back(row_value);

  *tuple = Tuple{output, &GetOutputSchema()};
  is_end_ = true;

  return true;
 }

}  // namespace bustub
