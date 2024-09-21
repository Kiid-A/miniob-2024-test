#pragma once

#include "sql/operator/logical_operator.h"

/**
 * @brief 逻辑算子，用于执行 update 语句
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<Value> values, std::vector<Field> fields);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  Table              *table() const { return table_; }
  std::vector<Value>  values() { return values_; }

  std::vector<Field> fields() const { return fields_; }

private:
  Table             *table_ = nullptr;
  std::vector<Value> values_;
  std::vector<Field> fields_;
};