#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),heap_(cmp_(plan->GetOrderBy(), plan->OutputSchema())) {
        plan_=plan;
        child_executor_=std::move(child_executor);
        }

void TopNExecutor::Init() { 
    if (is_fill_) {
    table_iter_ = tuples_.rbegin();
    num_in_heap_ = tuples_.size();
    return;
  }

  cmp_ custom_compare{plan_->GetOrderBy(), plan_->OutputSchema()};

  Tuple tuple;
  RID rid;
  child_executor_->Init();

  while (child_executor_->Next(&tuple, &rid)) {
    if (heap_.size() < plan_->GetN()) {
      heap_.push(tuple);
    } else if (!custom_compare(heap_.top(), tuple)) {
      heap_.pop();
      heap_.push(tuple);
    }
  }

  while (!heap_.empty()) {
    tuples_.push_back(heap_.top());
    heap_.pop();
  }
  is_fill_ = true;
  table_iter_ = tuples_.rbegin();
  num_in_heap_ = tuples_.size();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (table_iter_ == tuples_.rend())  return false;
  *tuple = *table_iter_;
  table_iter_++;
  num_in_heap_--;
  return true; }

auto TopNExecutor::GetNumInHeap() -> size_t { return num_in_heap_; };

}  // namespace bustub
