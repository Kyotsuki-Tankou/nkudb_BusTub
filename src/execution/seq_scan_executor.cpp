// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // seq_scan_executor.cpp
// //
// // Identification: src/execution/seq_scan_executor.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "execution/executors/seq_scan_executor.h"

// namespace bustub {

// SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx){plan_=plan;}

// void SeqScanExecutor::Init() { 
//     table_oid_t table_id = plan_->GetTableOid();
//     Catalog *catalog = exec_ctx_->GetCatalog();
//     TableInfo *table_info = catalog->GetTable(table_id);
//     auto &table_heap = table_info->table_;
//     table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
// }

// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
//     while (true) {
//     if (table_iter_->IsEnd()) {
//       return false;
//     }
    
//     TupleMeta tuple_meta = table_iter_->GetTuple().first;
//     // ++*table_iter_;

//     while(tuple_meta.is_deleted_)
//     {
//       ++(*table_iter_);
//       if(table_iter_->IsEnd())  return false;
//       tuple_meta=table_iter_->GetTuple().first;
//     }
//     *rid = table_iter_->GetRID();
//     *tuple = Tuple(table_iter_->GetTuple().second);
//     ++*table_iter_;
//     return true;
//   //   if (!tuple_meta.is_deleted_) {
//   //     // Make usage of the filter.
//   //     if (plan_->filter_predicate_) {
//   //       auto &filter_expr = plan_->filter_predicate_;
//   //       Value value = filter_expr->Evaluate(tuple, GetOutputSchema());
//   //       if (!value.IsNull() && value.GetAs<bool>()) {
//   //         return true;
//   //       }
//   //     } else {
//   //       return true;
//   //     }
//   //   }
//   // }
//   }
// }
// }  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstdio>
#include <memory>
#include <vector>
#include "catalog/catalog.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

// void SeqScanExecutor::Init() {
//   table_oid_t table_id = plan_->GetTableOid();
//   Catalog *catalog = exec_ctx_->GetCatalog();
//   TableInfo *table_info = catalog->GetTable(table_id);
//   auto &table_heap = table_info->table_;
//   table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
// }

// auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   while (true) {
//     if (table_iter_->IsEnd()) {
//       return false;
//     }
//     *rid = table_iter_->GetRID();
//     *tuple = Tuple(table_iter_->GetTuple().second);
//     TupleMeta tuple_meta = table_iter_->GetTuple().first;
//     ++*table_iter_; 

//     if (!tuple_meta.is_deleted_) {
//       // Make usage of the filter.
//       if (plan_->filter_predicate_) {
//         auto &filter_expr = plan_->filter_predicate_;
//         Value value = filter_expr->Evaluate(tuple, GetOutputSchema());
//         if (!value.IsNull() && value.GetAs<bool>()) {
//           return true;
//         }
//       } else {
//         return true;
//       }
//     }
//   }
// }

void SeqScanExecutor::Init() { 
    table_oid_t table_id = plan_->GetTableOid();
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(table_id);
    auto &table_heap = table_info->table_;
    table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while (true) {
        if (table_iter_->IsEnd()) {
            return false;
        }

        TupleMeta tuple_meta = table_iter_->GetTuple().first;
        *rid = table_iter_->GetRID();
        *tuple = Tuple(table_iter_->GetTuple().second);

        // Skip deleted tuples
        if (tuple_meta.is_deleted_) {
            ++*table_iter_;
            continue;
        }

        // Make usage of the filter.
        if (plan_->filter_predicate_) {
            auto &filter_expr = plan_->filter_predicate_;
            Value value = filter_expr->Evaluate(tuple, GetOutputSchema());
            if (!value.IsNull() && value.GetAs<bool>()) {
                ++*table_iter_;
                return true;
            }
        } else {
            ++*table_iter_;
            return true;
        }

        ++*table_iter_;
    }
}
}  // namespace bustub