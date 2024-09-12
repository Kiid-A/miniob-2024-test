/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by niuxn on 2022/6/27.
//

#include "common/log/log.h"
#include "sql/operator/update_physical_operator.h"
#include "common/rc.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/stmt/delete_stmt.h"

// a general thoughts to find target column and fetch value
RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  rc = find_fields();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to find column info: %s", strrc(rc));
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

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &old_record = row_tuple->record();
    Record backup_record(old_record);
    deleted_records_.push_back(backup_record);
    rc = table_->delete_record(old_record.rid());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to update record: %s", strrc(rc));
      rollback();
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

RC UpdatePhysicalOperator::find_fields()
{
  RC rc = RC::SUCCESS;
  for (size_t i = 0; i < fields_.size(); i++) {
    std::unique_ptr<Expression> &value_expr = values_[i];
    Value raw_value;
    rc = value_expr->try_get_value(raw_value);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to try get value");
      return rc;
    }

    if (raw_value.attr_type() != fields_[i].attr_type()) {
      Value result;
      if (Value::cast_to(const_cast<Value&>(raw_value), fields_[i].attr_type(), result) != RC::SUCCESS) {
        LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
                  table_->name(), fields_[i].field_name(), fields_[i].attr_type(), raw_value.attr_type());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    }

    raw_values_.push_back(raw_value);
  }

  return rc;
}

void UpdatePhysicalOperator::rollback()
{
  RC rc = RC::SUCCESS;
  for (auto &d : deleted_records_) {
    rc = table_->delete_record(d);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to delete record");
    }
  }

  for (auto &i : inserted_records_) {
    rc = table_->insert_record(i);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to insert record");
    }
  }
}

RC UpdatePhysicalOperator::make_record(Record &old_record, Record &new_record)
{
  RC rc = RC::SUCCESS;

  new_record.set_rid(old_record.rid());
  new_record.set_data(tmp_record_data_);
  memcpy(tmp_record_data_, old_record.data(), table_->table_meta().record_size());

  std::vector<Value> old_value;
  for (size_t c_idx = 0; c_idx < fields_.size(); c_idx++) {
    Value *value = &raw_values_[c_idx];
    const FieldMeta *field_meta = fields_[c_idx].meta();

    // 判断 新值与旧值是否相等，缓存旧值，将新值复制到新的record里
    const FieldMeta* null_field = table_->table_meta().null_field();
    common::Bitmap old_null_bitmap(old_record.data() + null_field->offset(), table_->table_meta().field_num());
    common::Bitmap new_null_bitmap(tmp_record_data_ + null_field->offset(), table_->table_meta().field_num());

    if (value->is_null() && old_null_bitmap.get_bit(fields_id_[c_idx])) {
      // 二者都是NULL，保存旧值即可
      old_value.emplace_back(NULLS, nullptr, 0);
    } else if (value->is_null()) {
      // 新值是NULL，旧值不是
      new_null_bitmap.set_bit(fields_id_[c_idx]);
      old_value.emplace_back(field_meta.type(), old_record.data() + field_meta.offset(), field_meta.len());
    } else {
      // 新值不是NULL
      new_null_bitmap.clear_bit(fields_id_[c_idx]);

      if (TEXTS == field_meta.type()) {
        int64_t position[2];
        position[1] = value->length();
        rc = table_->write_text(position[0], position[1], value->data());
        if (rc != RC::SUCCESS) {
          LOG_WARN("Failed to write text into table, rc=%s", strrc(rc));
          return rc;
        }
        memcpy(tmp_record_data_ + field_meta.offset(), position, 2 * sizeof(int64_t));       
      } else {
        memcpy(tmp_record_data_ + field_meta.offset(), value->data(), field_meta.len());   
      }

      if (old_null_bitmap.get_bit(fields_id_[c_idx])) {
        old_value.emplace_back(NULLS, nullptr, 0);
      } else if (TEXTS == field_meta.type()) {
        old_value.emplace_back(LONGS, old_record.data() + field_meta.offset(), sizeof(int64_t));
        old_value.emplace_back(LONGS, old_record.data() + field_meta.offset() + sizeof(int64_t), sizeof(int64_t));
      } else {
        old_value.emplace_back(field_meta.type(), old_record.data() + field_meta.offset(), field_meta.len());
      }
    }
  }
  // 比较整行数据
  if (0 == memcmp(old_record.data(), tmp_record_data_, table_->table_meta().record_size())) {
    LOG_WARN("update old value equals new value, skip this record");
    return RC::RECORD_DUPLICATE_KEY;
  }
  
  old_values_.emplace_back(std::move(old_value));
  old_rids_.emplace_back(old_record.rid());
  return rc;
}