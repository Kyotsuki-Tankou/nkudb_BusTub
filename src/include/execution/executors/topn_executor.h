//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

struct cmp_ {
  const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_;
  const Schema &schema_;

  cmp_(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_bys, const Schema &schema)
      : order_by_(order_bys), schema_(schema) {}

  auto operator()(const Tuple &a, const Tuple &b) const -> bool {
    bool equal = true;
    bool less = false;
    bool greater = false;

    for (const auto &_plan : order_by_) {
      const auto &val1 = _plan.second->Evaluate(&a, schema_);
      const auto &val2 = _plan.second->Evaluate(&b, schema_);
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
};

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;
  auto cmp(const Tuple &a, const Tuple &b) -> bool;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // max-top heap
  std::priority_queue<Tuple, std::vector<Tuple>, cmp_> heap_;
  std::vector<Tuple> tuples_{};
  std::vector<Tuple>::reverse_iterator table_iter_;
  size_t num_in_heap_{0};
  // fill tuples_
  bool is_fill_{false};
};
}  // namespace bustub
