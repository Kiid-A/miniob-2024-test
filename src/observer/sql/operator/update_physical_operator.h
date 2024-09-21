#pragma once

#include "sql/operator/physical_operator.h"

class Trx;
class DeleteStmt;

/**
 * @brief 物理算子，更新
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Table *table, std::vector<Value> values, std::vector<Field> fields)
      : table_(table), values_(values), fields_(fields)
  {}
  ~UpdatePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  Table                                 *table_ = nullptr;
  std::vector<Value>                     values_;
  std::vector<Field>                     fields_;
  Trx                                   *trx_ = nullptr;
  std::vector<std::pair<std::vector<char>, Record>> olds_;
};