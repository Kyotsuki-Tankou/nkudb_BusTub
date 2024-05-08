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

#include<iostream>
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx){plan_=plan;}

void SeqScanExecutor::Init() { 
    table_oid_t table_id = plan_->GetTableOid();
    Catalog *catalog = exec_ctx_->GetCatalog();
    TableInfo *table_info = catalog->GetTable(table_id);
    auto &table_heap = table_info->table_;
    table_iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    while(true)
    {
        if(table_iter_->IsEnd())  return false;
        *rid=table_iter_->GetRID();
        auto next_tuple=table_iter_->GetTuple();
        auto tuple_data=next_tuple.first;
        *tuple=Tuple(next_tuple.second);
        ++*table_iter_;
        if(!tuple_data.is_deleted_)
        {
            if(plan_->filter_predicate_)
            {
                auto &filter=plan_->filter_predicate_;
                Value val=filter->Evaluate(tuple,GetOutputSchema());
                if(!val.IsNull()&&val.GetAs<bool>())  return true;
            }
            else  return true;
        }
    }
}
}  // namespace bustub