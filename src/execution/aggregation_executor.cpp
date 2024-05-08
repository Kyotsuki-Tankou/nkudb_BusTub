//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),aht_(SimpleAggregationHashTable(plan->aggregates_,plan->agg_types_)),aht_iterator_(aht_.Begin())
     {plan_=plan;
     child_executor_=std::move(child_executor);}

void AggregationExecutor::Init() {
    is_end=false;
    aht_.GenerateInitialAggregateValue();
    Tuple tuple;
    RID rid;
    child_executor_->Init();
    while(child_executor_->Next(&tuple,&rid))
    {
        auto key_set=MakeAggregateKey(&tuple);
        auto value_set=MakeAggregateValue(&tuple);
        aht_.InsertCombine(key_set,value_set);
    }
    aht_iterator_=aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    output_vals.clear();
    while(true)
    {
        if(aht_iterator_==aht_.End())  break;
        output_vals.clear();
        auto agg_val=aht_iterator_.Val();
        auto agg_key=aht_iterator_.Key();
        output_vals.reserve(agg_val.aggregates_.size()+agg_key.group_bys_.size());
        for(auto &val:agg_key.group_bys_)  output_vals.emplace_back(val);
        for(auto &val:agg_val.aggregates_)  output_vals.emplace_back(val);
        ++aht_iterator_;
        *tuple=Tuple(output_vals,&GetOutputSchema());
        *rid=tuple->GetRid();
        return true;
    }
    if(is_end)  return false;
    if(aht_.Begin()==aht_.End())
    {
        output_vals.clear();
        if(is_end)  return false;
        if(!plan_->GetGroupBys().empty())  return false;
        auto agg_val=aht_.GenerateInitialAggregateValue();
        output_vals=agg_val.aggregates_;
        *tuple=Tuple(output_vals,&GetOutputSchema());
        *rid=tuple->GetRid();
        is_end=true;
        return true;
    }
    return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub