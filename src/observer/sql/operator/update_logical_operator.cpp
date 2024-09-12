#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(
    Table *table, std::vector<std::unique_ptr<Expression>> &values, std::vector<Field> &fields)
    : table_(table), values_(std::move(values)), fields_(fields)
{}