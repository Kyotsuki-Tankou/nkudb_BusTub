//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor->Init();
  // child_executor_=std::move(child_executor);

  Tuple tuple;
  RID rid;
  size_t i = 0;
  while (i < plan_->GetLimit() && child_executor->Next(&tuple, &rid)) {
    pairs_.emplace_back(tuple, rid);
    i++;
  }
}

void LimitExecutor::Init() { table_iter_ = pairs_.begin(); }

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == pairs_.end()) {
    return false;
  }
  *tuple = table_iter_->first;
  *rid = table_iter_->second;
  table_iter_++;
  return true;
}

}  // namespace bustub
