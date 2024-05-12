#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  is_fill_ = false;
}
auto SortExecutor::cmp(const Tuple &a, const Tuple &b) -> bool {
  bool equal = true;
  bool less = false;
  bool greater = false;

  for (const auto &_plan : plan_->GetOrderBy()) {
    const auto &val1 = _plan.second->Evaluate(&a, GetOutputSchema());
    const auto &val2 = _plan.second->Evaluate(&b, GetOutputSchema());
    if (val1.CompareEquals(val2) != CmpBool::CmpTrue) {
      equal = false;
      if (val1.CompareLessThan(val2) == CmpBool::CmpTrue) {
        less = true;
      } else {
        greater = true;
      }
      if (_plan.first == OrderByType::DESC) {
        less = !less;
        greater = !greater;
      }
    }
    if (!equal) break;
  }
  if (equal) return equal;
  if (less) return less;
  return false;
}

void SortExecutor::Init() {
  if (is_fill_) {
    table_iter_ = tuples_.begin();
    return;
  }

  child_executor_->Init();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &a, const Tuple &b) { return cmp(a, b); });

  is_fill_ = true;
  table_iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *table_iter_;
  table_iter_++;
  return true;
}

}  // namespace bustub
