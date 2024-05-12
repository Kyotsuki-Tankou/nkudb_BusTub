//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

class WinidowHashTable {
 public:
  explicit WinidowHashTable(const WindowFunctionType &window_type) : window_type_(window_type) {}

  auto GetFirstValueByKeyThenRemove(const AggregateKey &key) -> Value {
    assert(ht_.count(key) > 0);
    assert(!ht_.at(key).empty());

    auto &values = ht_.at(key);

    Value val = values.front();
    values.erase(values.begin());
    return val;
  }

  auto Insert(const AggregateKey &key, const Value &value) -> void {
    if (ht_.count(key) == 0) {
      ht_.insert({key, std::vector<Value>{}});
    }

    auto &values = ht_.at(key);

    switch (window_type_) {
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::CountStarAggregate:
        if (values.empty()) {
          values.push_back(ValueFactory::GetIntegerValue(1));
        } else {
          const auto &new_val = values.back().Add(ValueFactory::GetIntegerValue(1));
          values.push_back(new_val);
        }
        break;

      case WindowFunctionType::MaxAggregate:
        if (values.empty()) {
          values.push_back(value);
        } else {
          const auto &new_val = values.back().Max(value);
          values.push_back(new_val);
        }
        break;

      case WindowFunctionType::MinAggregate:
        if (values.empty()) {
          values.push_back(value);
        } else {
          const auto &new_val = values.back().Min(value);
          values.push_back(new_val);
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (values.empty()) {
          values.push_back(value);
        } else {
          const auto &new_val = values.back().Add(value);
          values.push_back(new_val);
        }
        break;
      case WindowFunctionType::Rank:
        // no deal
        throw NotImplementedException("no deal with Rank");
    }
  }

 private:
  std::unordered_map<AggregateKey, std::vector<Value>> ht_;
  const WindowFunctionType window_type_;
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }
  auto CompareCustom(const Tuple &a, const Tuple &b, const Schema &scheam,
                     const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by) const -> bool {
    bool equal = true;
    bool less = false;
    bool greater = false;

    for (const auto &iter_ : order_by) {
      const auto &val1 = iter_.second->Evaluate(&a, scheam);
      const auto &val2 = iter_.second->Evaluate(&b, scheam);

      if (val1.CompareEquals(val2) != CmpBool::CmpTrue) {
        equal = false;
        if (val1.CompareLessThan(val2) == CmpBool::CmpTrue)
          less = true;
        else
          greater = true;
        if (iter_.first == OrderByType::DESC) {
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

 private:
  auto BuildAggregateTables(const std::vector<Tuple> &child_tuples) const
      -> std::unordered_map<uint32_t, SimpleAggregationHashTable>;
  auto BuildWindowHashTables(const std::vector<Tuple> &child_tuples) const
      -> std::unordered_map<uint32_t, WinidowHashTable>;
  auto WindowType2AggregationType(WindowFunctionType type) const -> AggregationType;
  auto BuildChildExecutorTuples() -> std::vector<Tuple>;
  auto FillTuplesWithOutOrderBy(const std::vector<Tuple> &child_tuples) -> void;
  auto FillTuplesWithOrderBy(const std::vector<Tuple> &child_tuples) -> void;

  auto IsSame(const Tuple &a, const Tuple &b,
              const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by) const -> bool {
    return CompareCustom(a, b, child_executor_->GetOutputSchema(), order_by) &&
           CompareCustom(b, a, child_executor_->GetOutputSchema(), order_by);
  }

  auto MakeAggregateKey(const Tuple &tuple, const std::vector<AbstractExpressionRef> &partition_by) const
      -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by.size());
    for (const auto &expr : partition_by) {
      keys.push_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }
  auto MakeAggregateValue(const Tuple &tuple, const AbstractExpressionRef &function_arg) const -> AggregateValue {
    std::vector<Value> values;
    values.reserve(1);
    values.push_back(function_arg->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    return {values};
  }
  auto GetOrderBy() const -> const std::vector<std::pair<OrderByType, AbstractExpressionRef>> & {
    auto iter = plan_->window_functions_.begin();
    return iter->second.order_by_;
  }
  auto HasOrderBy() const -> bool { return !GetOrderBy().empty(); }
  const WindowFunctionPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> tuples_{};
  std::vector<Tuple>::const_iterator iter_;
  bool do_fill_;
};
}  // namespace bustub
