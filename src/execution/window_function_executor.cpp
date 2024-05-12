#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <optional>
#include <vector>
#include "common/exception.h"
#include "common/macros.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {do_fill_=false;}

void WindowFunctionExecutor::Init() {
  if (do_fill_) {
    iter_ = tuples_.cbegin();
    return;
  }
  const auto &child_tuples = BuildChildExecutorTuples();
  if (HasOrderBy())  FillTuplesWithOrderBy(child_tuples);
  else  FillTuplesWithOutOrderBy(child_tuples);
  do_fill_ = true;
  iter_ = tuples_.cbegin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(iter_ == tuples_.end())  return false;
    *tuple = *iter_;
    iter_++;
    return true; 
}

auto WindowFunctionExecutor::BuildChildExecutorTuples() -> std::vector<Tuple> {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  std::vector<Tuple> child_tuples{};

  while (child_executor_->Next(&tuple, &rid)) {
    child_tuples.push_back(tuple);
  }

  const auto &order_by = GetOrderBy();

  if (!order_by.empty()) {
    std::sort(child_tuples.begin(), child_tuples.end(), [this, &order_by](const Tuple &a, const Tuple &b) {
      return CompareCustom(a, b, child_executor_->GetOutputSchema(), order_by);
    });
  }

  return child_tuples;
}

auto WindowFunctionExecutor::BuildAggregateTables(const std::vector<Tuple> &child_tuples) const
    -> std::unordered_map<uint32_t, SimpleAggregationHashTable> {
  std::unordered_map<uint32_t, SimpleAggregationHashTable> hts;

  for (const auto &pair : plan_->window_functions_) {
    uint32_t index = pair.first;
    auto &window_func = pair.second;

    // no deal with RANK
    if (window_func.type_ == WindowFunctionType::Rank) {
      continue;
    }

    std::vector<AbstractExpressionRef> agg_exprs{window_func.function_};
    std::vector<AggregationType> agg_types{WindowType2AggregationType(window_func.type_)};
    SimpleAggregationHashTable ht(agg_exprs, agg_types);

    // deal with aggregate
    for (const auto &tuple : child_tuples) {
      const auto &key = MakeAggregateKey(tuple, window_func.partition_by_);
      const auto &value = MakeAggregateValue(tuple, window_func.function_);
      ht.InsertCombine(key, value);
    }

    hts.insert({index, ht});
  }

  return hts;
}

auto WindowFunctionExecutor::WindowType2AggregationType(WindowFunctionType type) const -> AggregationType {
  switch (type) {
    case WindowFunctionType::CountAggregate:
      return AggregationType::CountAggregate;
    case WindowFunctionType::CountStarAggregate:
      return AggregationType::CountStarAggregate;
    case WindowFunctionType::MaxAggregate:
      return AggregationType::MaxAggregate;
    case WindowFunctionType::MinAggregate:
      return AggregationType::MinAggregate;
    case WindowFunctionType::SumAggregate:
      return AggregationType::SumAggregate;
    case WindowFunctionType::Rank:
      throw NotImplementedException("Rank exception");
  }

  std::cout << "Exception~~~~~"
            << "\n";
  return AggregationType::SumAggregate;  // invalid
}

auto WindowFunctionExecutor::FillTuplesWithOutOrderBy(const std::vector<Tuple> &child_tuples) -> void {
  // build aggregation_hash_tables for each parition_by_
  const auto &hts = BuildAggregateTables(child_tuples);

  for (const auto &child_tuple : child_tuples) {
    std::vector<Value> values{};

    for (size_t i = 0; i < plan_->columns_.size(); i++) {
      const auto &expr = plan_->columns_.at(i);
      auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      assert(column_value_expr != nullptr);

      // placeholder: window function
      if (column_value_expr->GetColIdx() == UINT32_MAX) {
        const auto &window = plan_->window_functions_.at(i);
        BUSTUB_ASSERT(window.type_ != WindowFunctionType::Rank, "no order_by, no Rank");

        const auto &ht = hts.at(i);
        const auto &key = MakeAggregateKey(child_tuple, window.partition_by_);
        const auto &value = ht.GetAggregateValue(key);

        assert(value.aggregates_.size() == 1);
        values.push_back(value.aggregates_.at(0));

      } else {
        values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }
    }

    tuples_.emplace_back(values, &GetOutputSchema());
  }
}

auto WindowFunctionExecutor::FillTuplesWithOrderBy(const std::vector<Tuple> &child_tuples) -> void {
  auto whts = BuildWindowHashTables(child_tuples);

  std::optional<Tuple> last_tuple;
  int same = 0;
  int last_rank;

  for (const auto &child_tuple : child_tuples) {
    std::vector<Value> values{};

    for (size_t i = 0; i < plan_->columns_.size(); i++) {
      const auto &expr = plan_->columns_.at(i);
      auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
      assert(column_value_expr != nullptr);

      // indicate it is a placeholder(window function)
      if (column_value_expr->GetColIdx() == UINT32_MAX) {
        const auto &window = plan_->window_functions_.at(i);

        // latent bug: if have multi Rank, it will be wrong, so only support one Rank in a row by now
        if (window.type_ == WindowFunctionType::Rank) {
          if (!last_tuple.has_value()) {
            last_rank = 1;
            values.push_back(ValueFactory::GetIntegerValue(last_rank));
            same = 1;

          } else if (IsSame(child_tuple, last_tuple.value(), window.order_by_)) {
            values.push_back(ValueFactory::GetIntegerValue(last_rank));
            same++;

          } else {
            BUSTUB_ASSERT(
                !CompareCustom(child_tuple, last_tuple.value(), child_executor_->GetOutputSchema(), window.order_by_),
                "child_tuple must greater than last_tuple");
            last_rank += same;
            values.push_back(ValueFactory::GetBigIntValue(last_rank));
            same = 1;
          }

          last_tuple = std::make_optional(child_tuple);

        } else {
          const auto &key = MakeAggregateKey(child_tuple, window.partition_by_);
          auto &wht = whts.at(i);
          const auto &val = wht.GetFirstValueByKeyThenRemove(key);
          values.push_back(val);
        }

      } else {
        values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }
    }

    tuples_.emplace_back(values, &GetOutputSchema());
  }
}

auto WindowFunctionExecutor::BuildWindowHashTables(const std::vector<Tuple> &child_tuples) const
    -> std::unordered_map<uint32_t, WinidowHashTable> {
  std::unordered_map<uint32_t, WinidowHashTable> whts;

  for (const auto &pair : plan_->window_functions_) {
    uint32_t index = pair.first;
    auto &window_func = pair.second;

    // no deal with RANK
    if (window_func.type_ == WindowFunctionType::Rank) {
      continue;
    }

    WinidowHashTable wht{window_func.type_};
    for (const auto &child_tuple : child_tuples) {
      const auto &key = MakeAggregateKey(child_tuple, window_func.partition_by_);
      const auto &val = window_func.function_->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
      wht.Insert(key, val);
    }

    whts.insert({index, wht});
  }

  return whts;
}
}  // namespace bustub
