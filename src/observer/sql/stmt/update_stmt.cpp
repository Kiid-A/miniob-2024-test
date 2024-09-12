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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/select_stmt.h"

UpdateStmt::UpdateStmt(
    Table *table, std::vector<FieldMeta> fields, std::vector<unique_ptr<Expression>> values, FilterStmt *filter_stmt)
    : table_(table), values_(std::move(values)), fields_(std::move(fields)), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

// update set t1=t, t2=u
RC UpdateStmt::create(Db *db, UpdateSqlNode &updates, Stmt *&stmt)
{
  // find valid table
  const char *table_name = updates.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }
  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  std::vector<unique_ptr<Expression>> values;
  std::vector<FieldMeta>              fields;
  const int                           value_num  = static_cast<int>(updates.values.size());
  const TableMeta                    &table_meta = table->table_meta();
  const int                           field_num  = updates.attribute_names.size();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // move fields
  for (size_t i = 0; i < updates.attribute_names.size(); i++) {
    const FieldMeta *field = table_meta.field(updates.attribute_names[i].c_str());
    // check type
    if (nullptr == field) {
      LOG_WARN("no such field in table. db=%s, table=%s, field name=%s", 
             db->name(), table_name, updates.attribute_names[i].c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }

    /* TODO:subquery */
    values.emplace_back(updates.values[i]);
    fields.emplace_back(*field);
  }

  unordered_map<string, Table *> table_map;
  table_map.insert({table_name, table});

  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(
      db, table, &table_map, updates.conditions.data(), static_cast<int>(updates.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  stmt = new UpdateStmt(table, fields, values, filter_stmt);
  return RC::SUCCESS;
}