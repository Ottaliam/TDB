#pragma once

#include <map>
#include <functional>
#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"
#include "include/query_engine/structor/tuple/row_tuple.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/storage_engine/recorder/field.h"

class HashJoinPhysicalOperator : public PhysicalOperator
{
public:
  HashJoinPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&conditions, unique_ptr<ComparisonExpr> &&hash_condition);
  ~HashJoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  RC hash(Tuple *tuple, bool onright, size_t &result);

private:
  std::vector<std::unique_ptr<Expression>> conditions_;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  unique_ptr<ComparisonExpr> hash_condition_;   // 用于hash的等值条件
  std::unordered_multimap<size_t, unique_ptr<Record>> hash_table_;
  std::unordered_multimap<size_t, unique_ptr<Record>>::iterator current_match_;
  std::unordered_multimap<size_t, unique_ptr<Record>>::iterator end_match_;
  RowTuple *right_tuple_ = nullptr;
  Tuple *left_tuple_ = nullptr;
};
