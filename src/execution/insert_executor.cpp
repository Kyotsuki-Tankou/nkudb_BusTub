//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

#include <iostream>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
        plan_=plan;
        child_executor_=std::move(child_executor);
        table_info_=exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
    }
void InsertExecutor::Init() { 
    // child_executor_->Init();
    // sum_=0;
    // Catalog *catalog = exec_ctx_->GetCatalog();
    // table_info_ = catalog->GetTable(plan_->GetTableOid());
    row_amount_ = 0;
    row_value_ = Value(INTEGER, row_amount_);
    child_executor_->Init();
    table_oid_t table_id = plan_->GetTableOid();
    Catalog *catalog = exec_ctx_->GetCatalog();
    table_info_ = catalog->GetTable(table_id);
    index_array_ = catalog->GetTableIndexes(table_info_->name_);
    is_end_ = false;
 }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    // if(ending_)  return false;
    // Tuple value_tuple{};
    // std::vector<Value> values;
    // // auto table_id=plan_->table_oid_;
    // auto table_name=table_info_->name_;

    // auto status=child_executor_->Next(&value_tuple,rid);
    // TupleMeta inserted_tuple_meta;
    // auto index_array_ = GetExecutorContext()->GetCatalog()->GetTableIndexes(table_name);
    // timestamp_t insert_time=0;
    // while(status)
    // {
    //     inserted_tuple_meta.ts_ = insert_time;
    //     inserted_tuple_meta.is_deleted_ = false;
    //     auto new_rid=table_info_->table_->InsertTuple(inserted_tuple_meta, value_tuple, exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction(), table_info_->oid_);
    //     if (new_rid == std::nullopt) {
    //         return false;
    //     }
    //     if (!new_rid.has_value() || new_rid->GetPageId() == INVALID_PAGE_ID) {
    //         values.push_back(Value(INTEGER,sum_));
    //         Tuple output_tuple{values, &GetOutputSchema()};
    //         return false;
    //     }
    //     for(auto index_info:index_array_)
    //     {
    //         index_info->index_->InsertEntry(
    //         value_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
    //         new_rid.value(), GetExecutorContext()->GetTransaction());
    //     }
    //     sum_++;
    //     status=child_executor_->Next(&value_tuple,rid);
    // }
    // values.push_back(Value(INTEGER,sum_));
    // Tuple output_tuple{values, &GetOutputSchema()};
    // *tuple = output_tuple;
    
    // ending_ = true;
    // return true;


//     if (is_end_) {
//     return false;
//   }
//   Tuple child_tuple{};
//   auto &table_heap = table_info_->table_;
//   TupleMeta inserted_tuple_meta;
//   while (child_executor_->Next(&child_tuple, rid)) {
//     /** Insert tuple into the table. */
//     inserted_tuple_meta.ts_ = 0;
//     inserted_tuple_meta.is_deleted_ = false;

//     auto new_rid = table_heap->InsertTuple(inserted_tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
//                                            exec_ctx_->GetTransaction(), table_info_->oid_);
//     if (new_rid == std::nullopt) {
//       return false;
//     }

//     /** Update the affected indexes. */
//     for (auto &affected_index : index_array_) {
//       affected_index->index_->InsertEntry(child_tuple.KeyFromTuple(table_info_->schema_, affected_index->key_schema_,
//                                                                    affected_index->index_->GetKeyAttrs()),
//                                           new_rid.value(), exec_ctx_->GetTransaction());
//     }

//     row_amount_++;
//   }
//   row_value_ = Value(INTEGER, row_amount_);
//   std::vector<Value> output{};
//   output.reserve(GetOutputSchema().GetColumnCount());
//   output.push_back(row_value_);

//   *tuple = Tuple{output, &GetOutputSchema()};
//   is_end_ = true;

//   return true;

if (is_end_) {
        return false;
    }
    Tuple child_tuple{};
    auto &table_heap = table_info_->table_;
    TupleMeta inserted_tuple_meta;
    while (child_executor_->Next(&child_tuple, rid)) {
        /** Insert tuple into the table. */
        inserted_tuple_meta.ts_ = 0;
        inserted_tuple_meta.is_deleted_ = false;

        auto new_rid = table_heap->InsertTuple(inserted_tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                               exec_ctx_->GetTransaction(), table_info_->oid_);
        if (new_rid == std::nullopt) {
            continue;  // Skip this iteration if new_rid is nullopt
        }

        /** Update the affected indexes. */
        for (auto &affected_index : index_array_) {
            affected_index->index_->InsertEntry(child_tuple.KeyFromTuple(table_info_->schema_, affected_index->key_schema_,
                                                                       affected_index->index_->GetKeyAttrs()),
                                              new_rid.value(), exec_ctx_->GetTransaction());
        }

        row_amount_++;
    }
    row_value_ = Value(INTEGER, row_amount_);
    std::vector<Value> output{};
    output.reserve(GetOutputSchema().GetColumnCount());
    output.push_back(row_value_);

    *tuple = Tuple{output, &GetOutputSchema()};
    is_end_ = true;

    return true;
 }

}  // namespace bustub
