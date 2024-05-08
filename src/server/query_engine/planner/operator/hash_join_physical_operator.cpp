#include "include/query_engine/planner/operator/hash_join_physical_operator.h"

HashJoinPhysicalOperator::HashJoinPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&conditions, unique_ptr<ComparisonExpr> &&hash_condition) : 
    conditions_(std::move(conditions)), hash_condition_(std::move(hash_condition))
{
    for (auto &condition : conditions) {
        ASSERT(condition->value_type() == BOOLEANS, "join condition should be BOOLEAN type");
    }
}

RC HashJoinPhysicalOperator::open(Trx *trx)
{
    RC rc = RC::SUCCESS;

    // 初始化左右子运算符
    children_[0]->open(trx);
    if (rc != RC::SUCCESS) {
        return rc;
    }
    children_[1]->open(trx);
    if (rc != RC::SUCCESS) {
        return rc;
    }

    // 构建哈希表
    // current_tuple()总是指向同一个地址
    // current_tuple()->record()也是同一个地址
    // 因此选择复制所有的record()存储
    // 使用右侧节点构建哈希表，因为右侧节点返回的一定是RowTuple，不需要dynamic_cast判断类型
    while (children_[1]->next() == RC::SUCCESS) {
        if (right_tuple_ == nullptr) {
            // 拷贝一下，主要是为了set_schema，否则后面的_set_record无法进行
            right_tuple_ = new RowTuple(*static_cast<RowTuple*>(children_[1]->current_tuple()));
        }

        unique_ptr<Record> right_record = make_unique<Record>(static_cast<RowTuple*>(children_[1]->current_tuple())->record());

        right_tuple_->_set_record(right_record.get());
        
        size_t key;
        hash(right_tuple_, true/*onright*/, key);

        hash_table_.emplace(key, std::move(right_record));
    }

    for (auto it = hash_table_.begin(); it != hash_table_.end(); ++it) {
        right_tuple_->_set_record(it->second.get());
    }

    return rc;
}

RC HashJoinPhysicalOperator::next()
{
    RC rc = RC::SUCCESS;

    while (true) {
        // 当前没有正在进行的匹配
        if (left_tuple_ == nullptr || current_match_ == end_match_) {
            rc = children_[0]->next();
            if (rc != RC::SUCCESS) {
                return rc;  // 左侧已无更多元组，join结束
            }

            left_tuple_ = children_[0]->current_tuple();
            
            size_t key;
            hash(left_tuple_, false/*not onright*/, key);
            
            auto range = hash_table_.equal_range(key);
            if (range.first == range.second) {
                continue;   // 没有找到匹配项，继续下一个右侧元组
            }

            current_match_ = range.first;
            end_match_ = range.second;
        }

        // hash匹配成功，检查是否实际满足条件
        while (current_match_ != end_match_) {
            right_tuple_->_set_record(current_match_->second.get());

            joined_tuple_.set_left(left_tuple_);
            joined_tuple_.set_right(right_tuple_);

            Value value;
            
            // 首先检查hash条件
            rc = hash_condition_->get_value(joined_tuple_, value);
            if (rc != RC::SUCCESS) {
                return rc;
            }
            if (!value.get_boolean()) {
                ++current_match_;
                continue;
            }

            // 检查其他条件
            bool ok = true;
            for (auto &condition : conditions_) {
                rc = condition->get_value(joined_tuple_, value);
                if (rc != RC::SUCCESS) {
                    return rc;
                }

                if (!value.get_boolean()) {
                    ok = false;
                    break;
                }
            }

            ++current_match_;

            if (ok) {
                return RC::SUCCESS;
            } else {
                continue;
            }
        }
    }
}

RC HashJoinPhysicalOperator::close()
{
    children_[0]->close();
    children_[1]->close();
    hash_table_.clear();
    return RC::SUCCESS;
}

Tuple *HashJoinPhysicalOperator::current_tuple()
{
    return &joined_tuple_;
}

RC HashJoinPhysicalOperator::hash(Tuple *tuple, bool onright, size_t &result)
{
    Value value;

    if (!onright) {
        hash_condition_->left()->get_value(*tuple, value);
    } else {
        hash_condition_->right()->get_value(*tuple, value);
    }

    switch (value.attr_type()) {
        case AttrType::INTS:
            result = std::hash<int>()(value.get_int());
            return RC::SUCCESS;
        case AttrType::FLOATS:
            result = std::hash<float>()(value.get_float());
            return RC::SUCCESS;
        case AttrType::BOOLEANS:
            result = std::hash<bool>()(value.get_boolean());
            return RC::SUCCESS;
        case AttrType::CHARS:
        case AttrType::TEXTS:
        case AttrType::DATES:
            result = std::hash<std::string>()(value.get_string());
            return RC::SUCCESS;
        default:
            LOG_WARN("unknown data type. type=%d", value.attr_type());
            result = 0;
            return RC::INTERNAL;
    }
}