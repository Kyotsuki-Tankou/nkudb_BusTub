// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // hash_join_executor.h
// //
// // Identification: src/include/execution/executors/hash_join_executor.h
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #pragma once

// #include <memory>
// #include <utility>

// #include "execution/executor_context.h"
// #include "execution/executors/abstract_executor.h"
// #include "execution/plans/hash_join_plan.h"
// #include "storage/table/tuple.h"

// namespace bustub {

// /**
//  * HashJoinExecutor executes a nested-loop JOIN on two tables.
//  */
// class HashJoinExecutor : public AbstractExecutor {
//  public:
//   /**
//    * Construct a new HashJoinExecutor instance.
//    * @param exec_ctx The executor context
//    * @param plan The HashJoin join plan to be executed
//    * @param left_child The child executor that produces tuples for the left side of join
//    * @param right_child The child executor that produces tuples for the right side of join
//    */
//   HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
//                    std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

//   /** Initialize the join */
//   void Init() override;

//   /**
//    * Yield the next tuple from the join.
//    * @param[out] tuple The next tuple produced by the join.
//    * @param[out] rid The next tuple RID, not used by hash join.
//    * @return `true` if a tuple was produced, `false` if there are no more tuples.
//    */
//   auto Next(Tuple *tuple, RID *rid) -> bool override;

//   /** @return The output schema for the join */
//   auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

//  private:
//   /** The HashJoin plan node to be executed. */
//   const HashJoinPlanNode *plan_;
// };

// }  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "common/macros.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
namespace bustub {
struct HashJoinKey {
  // values as key in tuple
  std::vector<Value> values_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.values_.size(); ++i) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hjk) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hjk.values_) {
      BUSTUB_ASSERT(!key.IsNull(), "key is null");
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  // hash table for right executor
  std::unordered_multimap<HashJoinKey, Tuple, std::hash<HashJoinKey>> hjt_{};
  std::unordered_multimap<HashJoinKey, Tuple>::iterator hjt_iterator_;
  std::pair<std::unordered_multimap<HashJoinKey, Tuple>::iterator, std::unordered_multimap<HashJoinKey, Tuple>::iterator> hjt_range_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  const Schema *const left_schema_;
  const Schema *const right_schema_;
  std::vector<Value> values_;
  bool traversing_bucket_ = false;
};

}  // namespace bustub