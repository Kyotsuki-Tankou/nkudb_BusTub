// #include <algorithm>
// #include <memory>
// #include "catalog/column.h"
// #include "catalog/schema.h"
// #include "common/exception.h"
// #include "common/macros.h"
// #include "execution/expressions/column_value_expression.h"
// #include "execution/expressions/comparison_expression.h"
// #include "execution/expressions/constant_value_expression.h"
// #include "execution/plans/abstract_plan.h"
// #include "execution/plans/filter_plan.h"
// #include "execution/plans/hash_join_plan.h"
// #include "execution/plans/nested_loop_join_plan.h"
// #include "execution/plans/projection_plan.h"
// #include "optimizer/optimizer.h"
// #include "type/type_id.h"

// namespace bustub {

// auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
//   // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
//   // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
//   // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
//   return plan;
// }

// }  // namespace bustub
#include <algorithm>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
auto OptimizeSingleExpression(const ComparisonExpression *expr, const NestedLoopJoinPlanNode &nlj_plan,
                              std::vector<AbstractExpressionRef> &left_exprs,
                              std::vector<AbstractExpressionRef> &right_exprs) -> bool;

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have 2 children");

    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      // Handle multiple equi-conditions
      bool can_optimize = true;
      for (const auto &child_expr : expr->children_) {
        const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(child_expr.get());
        if (comp_expr == nullptr || !OptimizeSingleExpression(comp_expr, nlj_plan, left_exprs, right_exprs)) {
          can_optimize = false;
          break;
        }
      }
      if (can_optimize) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
      }
    } else if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());
               expr != nullptr) {
      if (OptimizeSingleExpression(expr, nlj_plan, left_exprs, right_exprs)) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                  std::move(right_exprs), nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}


auto OptimizeSingleExpression(const ComparisonExpression *expr, const NestedLoopJoinPlanNode &nlj_plan,
                              std::vector<AbstractExpressionRef> &left_exprs,
                              std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  if (expr->comp_type_ == ComparisonType::Equal) {
    if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
        left_expr != nullptr) {
      if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
          right_expr != nullptr) {
        // Ensure both exprs have tuple_id == 0
        auto left_expr_tuple_0 =
            std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
        auto right_expr_tuple_0 =
            std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
        // Now it's in form of <column_expr> = <column_expr>. Let's check if one of them is from the left table, and
        // the other is from the right table.
        if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
          left_exprs.push_back(std::move(left_expr_tuple_0));
          right_exprs.push_back(std::move(right_expr_tuple_0));
          return true;
        }
        if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
          right_exprs.push_back(std::move(left_expr_tuple_0));
          left_exprs.push_back(std::move(right_expr_tuple_0));
          return true;
        }
      }
    }
  }
  return false;
}

}  // namespace bustub