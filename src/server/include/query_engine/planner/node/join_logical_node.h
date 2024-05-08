#pragma once

 #include <memory>
#include "logical_node.h"

//TODO [Lab3] 请根据需要实现自己的JoinLogicalNode，当前实现仅为建议实现
class JoinLogicalNode : public LogicalNode
{
public:
  JoinLogicalNode() = default;
  ~JoinLogicalNode() override = default;

  LogicalNodeType type() const override
  {
    return LogicalNodeType::JOIN;
  }

  void add_condition(std::unique_ptr<Expression> &&condition)
  {
    condition_.push_back(std::move(condition));
  }

  std::vector<std::unique_ptr<Expression>> &condition()
  {
    return condition_;
  }
private:
  // Join的条件，目前只支持等值连接
  // 多个条件之间视作用AND连接，其中第一个元素[0]是解析出的条件，后续元素是可能的下推条件
  std::vector<std::unique_ptr<Expression>> condition_;
};
