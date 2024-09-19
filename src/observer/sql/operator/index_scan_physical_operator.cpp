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
// Created by Wangyunlai on 2022/07/08.
//

#include "sql/operator/index_scan_physical_operator.h"
#include "common/type/attr_type.h"
#include "storage/index/index.h"
#include "storage/trx/trx.h"

IndexScanPhysicalOperator::IndexScanPhysicalOperator(Table *table, Index *index, ReadWriteMode mode,
    const Value *left_value, bool left_inclusive, const Value *right_value, bool right_inclusive)
    : table_(table), index_(index), mode_(mode), left_inclusive_(left_inclusive), right_inclusive_(right_inclusive)
{
  if (left_value) {
    left_value_ = *left_value;
  }
  if (right_value) {
    right_value_ = *right_value;
  }
}

RC IndexScanPhysicalOperator::make_data(
    const std::vector<Value> &values, std::vector<FieldMeta> &meta, Table *table, char *out)
{
  std::vector<char> ret;
  int size = 0;
  for (auto &field : meta) {
    size += field.len();
  }
  ret.resize(size);
  out = ret.data();
  for (int i = 0; i < values.size() && i < meta.size(); i++) {
    Value &value = const_cast<Value &>(values[i]);
    memcpy(out, value.data(), meta[i].len());
    out += meta[i].len();
  }
  return RC::SUCCESS;
}

IndexScanPhysicalOperator::IndexScanPhysicalOperator(Table *table, Index *index, ReadWriteMode mode,
    const std::vector<Value> &left_value, bool left_inclusive, const std::vector<Value> &right_value,
    bool right_inclusive)
    : table_(table), index_(index), mode_(mode), left_inclusive_(left_inclusive), right_inclusive_(right_inclusive)
{
  std::vector<FieldMeta> fields = index_->field_metas();
  left_value_.set_type(AttrType::CHARS);
  right_value_.set_type(AttrType::CHARS);

  RC rc = RC::SUCCESS;
  int size = 0;
  for (auto &field : fields) {
    size += field.len();
  }
  char *ld = 0;
  rc = make_data(left_value, fields, table, ld);
  if (rc != RC::SUCCESS) {
    LOG_WARN("fail to make data");
  }
  left_value_.set_data(ld, size);
  char *rd = 0;
  rc = make_data(right_value, fields, table, rd);
  if (rc != RC::SUCCESS) {
    LOG_WARN("fail to make data");
  }
  right_value_.set_data(rd, size);

  free(ld);
  free(rd);
}

RC IndexScanPhysicalOperator::open(Trx *trx)
{
  if (nullptr == table_ || nullptr == index_) {
    return RC::INTERNAL;
  }

  IndexScanner *index_scanner = index_->create_scanner(left_value_.data(),
      left_value_.length(),
      left_inclusive_,
      right_value_.data(),
      right_value_.length(),
      right_inclusive_);
  if (nullptr == index_scanner) {
    LOG_WARN("failed to create index scanner");
    return RC::INTERNAL;
  }

  record_handler_ = table_->record_handler();
  if (nullptr == record_handler_) {
    LOG_WARN("invalid record handler");
    index_scanner->destroy();
    return RC::INTERNAL;
  }
  index_scanner_ = index_scanner;

  tuple_.set_schema(table_, table_->table_meta().field_metas());

  trx_ = trx;
  return RC::SUCCESS;
}

RC IndexScanPhysicalOperator::next()
{
  RID rid;
  RC  rc = RC::SUCCESS;

  bool filter_result = false;
  while (RC::SUCCESS == (rc = index_scanner_->next_entry(&rid))) {
    rc = record_handler_->get_record(rid, current_record_);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to get record. rid=%s, rc=%s", rid.to_string().c_str(), strrc(rc));
      return rc;
    }

    LOG_TRACE("got a record. rid=%s", rid.to_string().c_str());

    tuple_.set_record(&current_record_);
    rc = filter(tuple_, filter_result);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to filter record. rc=%s", strrc(rc));
      return rc;
    }

    if (!filter_result) {
      LOG_TRACE("record filtered");
      continue;
    }

    rc = trx_->visit_record(table_, current_record_, mode_);
    if (rc == RC::RECORD_INVISIBLE) {
      LOG_TRACE("record invisible");
      continue;
    } else {
      return rc;
    }
  }

  return rc;
}

RC IndexScanPhysicalOperator::close()
{
  index_scanner_->destroy();
  index_scanner_ = nullptr;
  return RC::SUCCESS;
}

Tuple *IndexScanPhysicalOperator::current_tuple()
{
  tuple_.set_record(&current_record_);
  return &tuple_;
}

void IndexScanPhysicalOperator::set_predicates(std::vector<std::unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

RC IndexScanPhysicalOperator::filter(RowTuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;
  for (std::unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      result = false;
      return rc;
    }
  }

  result = true;
  return rc;
}

std::string IndexScanPhysicalOperator::param() const
{
  return std::string(index_->index_meta().name()) + " ON " + table_->name();
}
