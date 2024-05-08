#include "include/query_engine/planner/operator/physical_operator_generator.h"

#include <cmath>
#include <utility>
#include "include/query_engine/planner/node/logical_node.h"
#include "include/query_engine/planner/operator/physical_operator.h"
#include "include/query_engine/planner/node/table_get_logical_node.h"
#include "include/query_engine/planner/operator/table_scan_physical_operator.h"
#include "include/query_engine/planner/operator/index_scan_physical_operator.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/join_physical_operator.h"
#include "include/query_engine/planner/operator/hash_join_physical_operator.h"
#include "include/query_engine/planner/node/predicate_logical_node.h"
#include "include/query_engine/planner/operator/predicate_physical_operator.h"
#include "include/query_engine/planner/node/order_by_logical_node.h"
#include "include/query_engine/planner/operator/order_physical_operator.h"
#include "include/query_engine/planner/node/project_logical_node.h"
#include "include/query_engine/planner/operator/project_physical_operator.h"
#include "include/query_engine/planner/node/aggr_logical_node.h"
#include "include/query_engine/planner/operator/aggr_physical_operator.h"
#include "include/query_engine/planner/node/insert_logical_node.h"
#include "include/query_engine/planner/operator/insert_physical_operator.h"
#include "include/query_engine/planner/node/delete_logical_node.h"
#include "include/query_engine/planner/operator/delete_physical_operator.h"
#include "include/query_engine/planner/node/update_logical_node.h"
#include "include/query_engine/planner/operator/update_physical_operator.h"
#include "include/query_engine/planner/node/explain_logical_node.h"
#include "include/query_engine/planner/operator/explain_physical_operator.h"
#include "include/query_engine/planner/node/join_logical_node.h"
#include "include/query_engine/planner/operator/group_by_physical_operator.h"
#include "include/query_engine/structor/expression/comparison_expression.h"
#include "include/query_engine/structor/expression/value_expression.h"
#include "common/log/log.h"
#include "include/storage_engine/recorder/table.h"

using namespace std;

RC PhysicalOperatorGenerator::create(LogicalNode &logical_operator, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  switch (logical_operator.type()) {
    case LogicalNodeType::TABLE_GET: {
      return create_plan(static_cast<TableGetLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::PREDICATE: {
      return create_plan(static_cast<PredicateLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::ORDER: {
      return create_plan(static_cast<OrderByLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::PROJECTION: {
      return create_plan(static_cast<ProjectLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::AGGR: {
      return create_plan(static_cast<AggrLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::INSERT: {
      return create_plan(static_cast<InsertLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::DELETE: {
      return create_plan(static_cast<DeleteLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalNode &>(logical_operator), oper);
    }

    case LogicalNodeType::EXPLAIN: {
      return create_plan(static_cast<ExplainLogicalNode &>(logical_operator), oper, is_delete);
    }
    // TODO [Lab3] 实现JoinNode到JoinOperator的转换
    case LogicalNodeType::JOIN: {
      return create_plan(static_cast<JoinLogicalNode &>(logical_operator), oper, is_delete);
    }

    case LogicalNodeType::GROUP_BY: {
      return RC::UNIMPLENMENT;
    }

    default: {
      return RC::INVALID_ARGUMENT;
    }
  }
}

// TODO [Lab2] 
// 在原有的实现中，会直接生成TableScanOperator对所需的数据进行全表扫描，但其实在生成执行计划时，我们可以进行简单的优化：
// 首先检查扫描的table是否存在索引，如果存在可以使用的索引，那么我们可以直接生成IndexScanOperator来减少磁盘的扫描
RC PhysicalOperatorGenerator::create_plan(
    TableGetLogicalNode &table_get_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<Expression>> &predicates = table_get_oper.predicates();
  Index *index = nullptr;
  // TODO [Lab2] 生成IndexScanOperator的准备工作,主要包含:
  // 1. 通过predicates获取具体的值表达式， 目前应该只支持等值表达式的索引查找
    // example:
    //  if(predicate.type == ExprType::COMPARE){
    //   auto compare_expr = dynamic_cast<ComparisonExpr*>(predicate.get());
    //   if(compare_expr->comp != EQUAL_TO) continue;
    //   [process]
    //  }
  // 2. 对应上面example里的process阶段， 找到等值表达式中对应的FieldExpression和ValueExpression(左值和右值)
  // 通过FieldExpression找到对应的Index, 通过ValueExpression找到对应的Value

  FieldExpr *field_expr = nullptr;
  ValueExpr *value_expr = nullptr;

  for (unique_ptr<Expression> &predicate : predicates) {
    if (predicate->type() == ExprType::COMPARISON) {
      auto compare_expr = dynamic_cast<ComparisonExpr*>(predicate.get());
      if (compare_expr->comp() == EQUAL_TO) {
        field_expr = dynamic_cast<FieldExpr*>(compare_expr->_left_().get());
        value_expr = dynamic_cast<ValueExpr*>(compare_expr->_right_().get());

        Table *table = table_get_oper.table();
        index = table->find_index_by_field(field_expr->field_name());
        if (index != nullptr) {
          break;
        }
      }
    }
  }

  if(index == nullptr){
    Table *table = table_get_oper.table();
    auto table_scan_oper = new TableScanPhysicalOperator(table, table_get_oper.table_alias(), table_get_oper.readonly());
    table_scan_oper->isdelete_ = is_delete;
    table_scan_oper->set_predicates(std::move(predicates));
    oper = unique_ptr<PhysicalOperator>(table_scan_oper);
    LOG_TRACE("use table scan");
  }else{
    // TODO [Lab2] 生成IndexScanOperator, 并放置在算子树上，下面是一个实现参考，具体实现可以根据需要进行修改
    // IndexScanner 在设计时，考虑了范围查找索引的情况，但此处我们只需要考虑单个键的情况
    // const Value &value = value_expression->get_value();
    // IndexScanPhysicalOperator *operator =
    //              new IndexScanPhysicalOperator(table, index, readonly, &value, true, &value, true);
    // oper = unique_ptr<PhysicalOperator>(operator);

    const Value &value = value_expr->get_value();
    Table *table = table_get_oper.table();
    IndexScanPhysicalOperator *index_scan_oper = new IndexScanPhysicalOperator(table, index, table_get_oper.readonly(), &value, true, &value, true);
    index_scan_oper->isdelete_ = is_delete;
    index_scan_oper->set_predicates(predicates);
    oper = unique_ptr<PhysicalOperator>(index_scan_oper);
    LOG_TRACE("use index scan");
  }

  return RC::SUCCESS;
}

static void join_condition_pushdown_left(vector<unique_ptr<LogicalNode>> &children_opers, ComparisonExpr *comp_expr)
{
  // JoinLogicalNode 的左侧子节点可能是TableGetLogicalNode或另一个JoinLogicalNode
  if (children_opers[0]->type() == LogicalNodeType::TABLE_GET) {
    auto left_child_oper = static_cast<TableGetLogicalNode*>(children_opers[0].get());
    left_child_oper->predicates().push_back(unique_ptr<Expression>(comp_expr));
  } else {
    auto left_child_oper = static_cast<JoinLogicalNode*>(children_opers[0].get());
    left_child_oper->add_condition(unique_ptr<Expression>(comp_expr));
  }
}

static void join_condition_pushdown_right(vector<unique_ptr<LogicalNode>> &children_opers, ComparisonExpr *comp_expr)
{
  // JoinLogicalNode 的右侧子节点一定是TableGetLogicalNode
  auto right_child_oper = static_cast<TableGetLogicalNode*>(children_opers[1].get());
  right_child_oper->predicates().push_back(unique_ptr<Expression>(comp_expr));
}

static void join_condition_pushdown(vector<unique_ptr<LogicalNode>> &children_opers, ComparisonExpr *comp_expr)
{
  if (comp_expr->left()->type() == ExprType::VALUE) {
    // Value = Value，恒真或横假，往一边下推即可，这里往可能更深的左侧下推
    join_condition_pushdown_left(children_opers, comp_expr);
  } else {
    // Field = Value  ||   Field = Field
    auto right_child_oper = static_cast<TableGetLogicalNode*>(children_opers[1].get());
    bool onright = (static_cast<FieldExpr*>(comp_expr->left().get())->field().table() == right_child_oper->table());

    if (onright) {
      join_condition_pushdown_right(children_opers, comp_expr);
    } else {
      join_condition_pushdown_left(children_opers, comp_expr);
    }
  }
}

static void process_hash_and_pushdown(
  vector<unique_ptr<Expression>> &condition_list, vector<unique_ptr<LogicalNode>> &children_opers,
  bool &use_hash, unique_ptr<ComparisonExpr> &hash_condition)
{
  for (auto it = condition_list.begin(); it != condition_list.end(); ) {
    if ((*it)->type() != ExprType::COMPARISON) {
      ++it;
      continue;
    }

    auto comp_expr = static_cast<ComparisonExpr*>(it->get());

    // 目前只考虑等值条件的优化
    if (comp_expr->comp() != CompOp::EQUAL_TO) {
      ++it;
      continue;
    }

    if (comp_expr->left()->type() == ExprType::FIELD && comp_expr->left()->type() == ExprType::FIELD) {
      // Field = Field
      
      // 首先检查是否在同侧子节点 (右侧子节点一定是TableGetLogicalNode)
      auto right_child_oper = static_cast<TableGetLogicalNode*>(children_opers[1].get());
      bool onright_1 = (static_cast<FieldExpr*>(comp_expr->left().get())->field().table() == right_child_oper->table());
      bool onright_2 = (static_cast<FieldExpr*>(comp_expr->right().get())->field().table() == right_child_oper->table());

      if (onright_1 == onright_2) {
        // 同侧，条件下推
        join_condition_pushdown(children_opers, static_cast<ComparisonExpr*>(it->release()));
        it = condition_list.erase(it);
      } else {
        // 异侧，本次join使用hash join加速
        if (use_hash) { // 如果已经设置了使用hash
          ++it;
          continue;
        } else {
          use_hash = true;
          hash_condition = unique_ptr<ComparisonExpr>(static_cast<ComparisonExpr*>(it->release()));
          if (onright_1) {
            // 确保表达式左右与操作符左右对应
            swap(hash_condition->left(), hash_condition->right());
          }
          it = condition_list.erase(it);
        }
      }
    } else if (comp_expr->left()->type() == ExprType::FIELD && comp_expr->left()->type() == ExprType::VALUE) {
      // Field = Value，条件下推
      join_condition_pushdown(children_opers, static_cast<ComparisonExpr*>(it->release()));
      it = condition_list.erase(it);
    } else if (comp_expr->left()->type() == ExprType::VALUE && comp_expr->left()->type() == ExprType::FIELD) {
      // Value = Field，左右交换之后条件下推
      swap(comp_expr->left(), comp_expr->right());
      join_condition_pushdown(children_opers, static_cast<ComparisonExpr*>(it->release()));
      it = condition_list.erase(it);
    } else if (comp_expr->left()->type() == ExprType::VALUE && comp_expr->left()->type() == ExprType::VALUE) {
      // Value = Value，条件下推
      join_condition_pushdown(children_opers, static_cast<ComparisonExpr*>(it->release()));
      it = condition_list.erase(it);
    } else {
      ++it;
      continue;
    }
  }
}

RC PhysicalOperatorGenerator::create_plan(
  JoinLogicalNode &join_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &children_opers = join_oper.children();
  ASSERT(children_opers.size() == 2, "join logical operator's sub oper number should be 2");

  /*
    !按照当前的语法解析&分析预处理实现，所有条件被视作用相同的运算符(AND | OR)连接，该连接符是条件最左边的那个
    INSERT INTO join_table_1 VALUES (1, 'a');
    INSERT INTO join_table_1 VALUES (2, 'b');
    INSERT INTO join_table_1 VALUES (3, 'c');
    TDB > select * from join_table_1 where id=2 or id=1 and name='c'
    id | name
    1 |    a
    2 |    b
    3 |    c
    !在当前的语法解析&分析预处理实现下，condition (即condition_[0]) 一定是若干ComparisonExpr的ConjunctionExpr
  */
  ConjunctionExpr *condition = static_cast<ConjunctionExpr*>(join_oper.condition()[0].get());

  bool use_hash = false;
  unique_ptr<ComparisonExpr> hash_condition;

  if (condition->conjunction_type() == ConjunctionType::AND) {  // 仅AND连接的条件可以优化
    process_hash_and_pushdown(condition->children(), children_opers, use_hash, hash_condition);
  }
  process_hash_and_pushdown(join_oper.condition(), children_opers, use_hash, hash_condition);

  if (use_hash) {
    LOG_TRACE("use hash join");
    oper = unique_ptr<PhysicalOperator>(new HashJoinPhysicalOperator(std::move(join_oper.condition()), std::move(hash_condition)));
  } else {
    LOG_TRACE("use nested loop join");
    oper = unique_ptr<PhysicalOperator>(new JoinPhysicalOperator(std::move(join_oper.condition())));
  }

  oper->isdelete_ = is_delete;
  for (size_t i = 0; i < 2; i++) {
    unique_ptr<PhysicalOperator> child_phy_oper;
    RC rc = create(*children_opers[i], child_phy_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
      return rc;
    }

    oper->add_child(std::move(child_phy_oper));
  }

  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(
    PredicateLogicalNode &pred_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &children_opers = pred_oper.children();
  ASSERT(children_opers.size() == 1, "predicate logical operator's sub oper number should be 1");

  LogicalNode &child_oper = *children_opers.front();

  unique_ptr<PhysicalOperator> child_phy_oper;
  RC rc = create(child_oper, child_phy_oper, is_delete);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create child operator of predicate operator. rc=%s", strrc(rc));
    return rc;
  }

  vector<unique_ptr<Expression>> &expressions = pred_oper.expressions();
  ASSERT(expressions.size() == 1, "predicate logical operator's children should be 1");

  unique_ptr<Expression> expression = std::move(expressions.front());

  oper = unique_ptr<PhysicalOperator>(new PredicatePhysicalOperator(std::move(expression)));
  oper->add_child(std::move(child_phy_oper));
  oper->isdelete_ = is_delete;
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(AggrLogicalNode &aggr_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = aggr_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *aggr_operator = new AggrPhysicalOperator(&aggr_oper);

  if (child_phy_oper) {
    aggr_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(aggr_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(OrderByLogicalNode &order_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = order_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  OrderPhysicalOperator* order_operator = new OrderPhysicalOperator(std::move(order_oper.order_units()));

  if (child_phy_oper) {
    order_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(order_operator);

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ProjectLogicalNode &project_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = project_oper.children();

  unique_ptr<PhysicalOperator> child_phy_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_phy_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create project logical operator's child physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  auto *project_operator = new ProjectPhysicalOperator(&project_oper);
  for (const auto &i : project_oper.expressions()) {
    project_operator->add_projector(i->copy());
  }

  if (child_phy_oper) {
    project_operator->add_child(std::move(child_phy_oper));
  }

  oper = unique_ptr<PhysicalOperator>(project_operator);
  oper->isdelete_ = is_delete;

  LOG_TRACE("create a project physical operator");
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(InsertLogicalNode &insert_oper, unique_ptr<PhysicalOperator> &oper)
{
  Table *table = insert_oper.table();
  vector<vector<Value>> multi_values;
  for (int i = 0; i < insert_oper.multi_values().size(); i++) {
    vector<Value> &values = insert_oper.values(i);
    multi_values.push_back(values);
  }
  InsertPhysicalOperator *insert_phy_oper = new InsertPhysicalOperator(table, std::move(multi_values));
  oper.reset(insert_phy_oper);
  return RC::SUCCESS;
}

RC PhysicalOperatorGenerator::create_plan(DeleteLogicalNode &delete_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = delete_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper, true);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new DeletePhysicalOperator(delete_oper.table()));
  oper->isdelete_ = true;
  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(UpdateLogicalNode &update_oper, unique_ptr<PhysicalOperator> &oper)
{
  vector<unique_ptr<LogicalNode>> &child_opers = update_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalNode *child_oper = child_opers.front().get();
    rc = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(update_oper.table(), update_oper.update_units()));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }
  return rc;
}

RC PhysicalOperatorGenerator::create_plan(
    ExplainLogicalNode &explain_oper, unique_ptr<PhysicalOperator> &oper, bool is_delete)
{
  vector<unique_ptr<LogicalNode>> &child_opers = explain_oper.children();

  RC rc = RC::SUCCESS;
  unique_ptr<PhysicalOperator> explain_physical_oper(new ExplainPhysicalOperator);
  for (unique_ptr<LogicalNode> &child_oper : child_opers) {
    unique_ptr<PhysicalOperator> child_physical_oper;
    rc = create(*child_oper, child_physical_oper, is_delete);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create child physical operator. rc=%s", strrc(rc));
      return rc;
    }

    explain_physical_oper->add_child(std::move(child_physical_oper));
  }

  oper = std::move(explain_physical_oper);
  oper->isdelete_ = is_delete;
  return rc;
}

// TODO [Lab3] 根据LogicalNode生成对应的PhyiscalOperator
RC PhysicalOperatorGenerator::create_plan(
    JoinLogicalNode &join_oper, unique_ptr<PhysicalOperator> &oper)
{
  return RC::UNIMPLENMENT;
}