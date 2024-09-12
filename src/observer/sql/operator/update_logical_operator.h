#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"
#include "common/value.h"
#include "storage/field/field.h"

//// 更新需要什么？目标列->目标值。
/** 所以我希望得到解析的 value: target
 *  具体的列: filter + field ?
 *  so, change target filed with the very value
 */

/**
 * @brief 更新逻辑算子
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<std::unique_ptr<Expression>> &values, std::vector<Field> &fields);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::UPDATE;
  }

  Table *table() const { return table_; }
  const std::vector<std::unique_ptr<Expression>> &values() const { return values_; }
  std::vector<std::unique_ptr<Expression>> &values() { return values_; }

  const std::vector<Field> &fields() const { return fields_; }
  std::vector<Field> &fields() { return fields_; }
  
private:
  Table *table_ = nullptr;
  std::vector<std::unique_ptr<Expression>> values_;
  std::vector<Field> fields_;
};