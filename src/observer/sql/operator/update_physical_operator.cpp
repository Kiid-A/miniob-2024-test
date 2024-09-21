#include "sql/operator/update_physical_operator.h"
#include "common/rc.h"
#include "sql/stmt/update_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple         *row_tuple = static_cast<RowTuple *>(tuple);
    Record           &record    = row_tuple->record();
    std::vector<char> updated_data(record.len());
    memcpy(updated_data.data(), record.data(), record.len());

    for (size_t i = 0; i < fields_.size(); i++) {
      auto field_meta = fields_[i].meta();
      auto value      = values_[i];
      if (field_meta == nullptr) {
        rc = RC::EMPTY;
        return rc;
      }

      int offset = field_meta->offset();
      int len    = field_meta->len();

      memcpy(updated_data.data() + offset, value.data(), len);
    }

    if (0 == memcmp(record.data(), updated_data.data(), table_->table_meta().record_size())) {
      LOG_WARN("update old value equals new value, skip this record");
      return RC::RECORD_DUPLICATE_KEY;
    }

    olds_.push_back({updated_data, record});

    rc = trx_->update_record(table_, record, updated_data.data());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      RC rc2 = RC::SUCCESS;
      olds_.pop_back();

      for (size_t i = 0; i < olds_.size(); i++) {
        auto v      = olds_[i].first;
        auto record = olds_[i].second;
        Record new_record = record;
        new_record.set_data_owner(v.data(), v.size());
        if (RC::SUCCESS != (rc2 = table_->update_record(new_record, record.data()))) {
          LOG_WARN("Failed to rollback record, rc=%s", strrc(rc2));
          break;
        }
      }
      return rc;
    }
  }
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}