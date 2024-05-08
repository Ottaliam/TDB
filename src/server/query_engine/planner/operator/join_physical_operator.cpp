#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&conditions) : conditions_(std::move(conditions))
{
  for (auto &condition : conditions) {
    ASSERT(condition->value_type() == BOOLEANS, "join condition should be BOOLEAN type");
  }
}

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;

  // 存储trx，因为执行过程涉及对子运算符的多次重置，需要多次open()
  trx_ = trx;

  // 初始化左右子运算符
  children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  children_[1]->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // 获取第一个左侧元组
  if (children_[0]->next() == RC::SUCCESS) {
    left_tuple_ = children_[0]->current_tuple();
  }

  return rc;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  if (left_tuple_ == nullptr) {
    // 如果一开始就没有左侧元组
    return RC::RECORD_EOF;
  }

  while (true) {
    // 尝试从右侧获取匹配元组
    while (children_[1]->next() == RC::SUCCESS) {
      right_tuple_ = children_[1]->current_tuple();
      
      joined_tuple_.set_left(left_tuple_);
      joined_tuple_.set_right(right_tuple_);

      Value value;
      bool ok = true;
      for (auto &condition : conditions_) {
        RC rc = condition->get_value(joined_tuple_, value);
        if (rc != RC::SUCCESS) {
            return rc;
        }

        if (!value.get_boolean()) {
            ok = false;
            break;
        }
      }

      if (ok) {
        return RC::SUCCESS;
      } else {
        continue;
      }
    }

    // 如果右侧没有更多元组，重置右侧操作符并从左侧获取下一个元组
    children_[1]->close();
    children_[1]->open(trx_);
    if (children_[0]->next() == RC::SUCCESS) {
      left_tuple_ = children_[0]->current_tuple();
    } else {
      left_tuple_ = nullptr;
      return RC::RECORD_EOF;
    }
  }
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  children_[0]->close();
  children_[1]->close();
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
