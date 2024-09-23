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
#include <string>
#include "common/log/log.h"
#include "common/rc.h"
#include "common/type/attr_type.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "deps/common/time/datetime.h"

UpdateStmt::UpdateStmt(Table *table, std::vector<Value> values, FilterStmt *filter_stmt, std::vector<Field> fields)
    : table_(table), filter_stmt_(filter_stmt), values_(values), fields_(fields)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{

  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || update.attribute_names.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p\n", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // 检查表格是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
  }

  // 生成过滤条件
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  std::vector<Value>       values          = update.values;
  std::vector<std::string> attribute_names = update.attribute_names;
  std::vector<Field>       fields;
  for (auto attr_name : attribute_names) {
    const FieldMeta *field_meta;
    field_meta = table->table_meta().field(attr_name.c_str());
    if (field_meta == nullptr) {
      LOG_WARN("field %s not exist in table %s", attr_name.c_str(), update.relation_name.c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    Field field = Field(table, field_meta);
    fields.push_back(field);
  }

  for (int i = 0; i < values.size(); i++) {
    if (values[i].attr_type() != fields[i].attr_type()) {
      return RC::INVALID_ARGUMENT;
    }
  }

  FilterStmt *filter = nullptr;
  if (!update.conditions.empty()) {
    rc = FilterStmt::create(db, table, &table_map, update.conditions.data(), update.conditions.size(), filter);
    if (rc != RC::SUCCESS) {
      LOG_WARN("fail to create filter stmt");
      return rc;
    }
  }

  stmt = new UpdateStmt(table, values, filter_stmt, fields);

  return RC::SUCCESS;
}
