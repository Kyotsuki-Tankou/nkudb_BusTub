//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

// namespace bustub {

// HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
//                                    std::unique_ptr<AbstractExecutor> &&left_child,
//                                    std::unique_ptr<AbstractExecutor> &&right_child)
//     : AbstractExecutor(exec_ctx) {
//   if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
//     // Note for 2023 Fall: You ONLY need to implement left join and inner join.
//     throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
//   }
// }

// void HashJoinExecutor::Init() { throw NotImplementedException("HashJoinExecutor is not implemented"); }

// auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

// }  // namespace bustub

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      left_schema_(&left_child_->GetOutputSchema()),
      right_schema_(&right_child_->GetOutputSchema()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  values_.clear();
  if (!hjt_.empty()) {
    return;
  }
  Tuple right_tuple{};
  RID dummy_rid{};
  std::vector<Value> right_key;
  while (right_child_->Next(&right_tuple, &dummy_rid)) {
    right_key.clear();
    for (auto &expr : plan_->right_key_expressions_) {
      right_key.push_back(expr->Evaluate(&right_tuple, plan_->GetRightPlan()->OutputSchema()));
    }
    hjt_.insert({{right_key}, right_tuple});
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!traversing_bucket_) {
      Tuple left_tuple{};
      values_.clear();
      if (!left_child_->Next(&left_tuple, rid)) {
        return false;
      }
      for (size_t i = 0; i < left_schema_->GetColumnCount(); ++i) {
        values_.push_back(left_tuple.GetValue(left_schema_, i));
      }
      std::vector<Value> left_key;
      left_key.reserve(plan_->left_key_expressions_.size());
      for (auto &expr : plan_->left_key_expressions_) {
        left_key.push_back(expr->Evaluate(&left_tuple, *left_schema_));
      }
      hjt_range_ = hjt_.equal_range({left_key});
      if (plan_->join_type_ == JoinType::LEFT && hjt_range_.first == hjt_.end()) {
        for (size_t i = 0; i < right_schema_->GetColumnCount(); ++i) {
          values_.push_back(ValueFactory::GetNullValueByType(right_schema_->GetColumn(i).GetType()));
        }
        *tuple = {values_, &GetOutputSchema()};
        return true;
      }
      hjt_iterator_ = hjt_range_.first;
      traversing_bucket_ = true;
    } else {
      if (hjt_iterator_ == hjt_range_.second) {
        traversing_bucket_ = false;
        continue;
      }
      Tuple right_tuple = hjt_iterator_->second;
      for (size_t i = 0; i < right_schema_->GetColumnCount(); ++i) {
        values_.push_back(right_tuple.GetValue(right_schema_, i));
      }
      ++hjt_iterator_;
      *tuple = {values_, &GetOutputSchema()};
      values_.resize(left_schema_->GetColumnCount());
      values_.reserve(GetOutputSchema().GetColumnCount());
      return true;
    }
  }
  return false;
}

}  // namespace bustub