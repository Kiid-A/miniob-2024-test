/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/rc.h"
#include "common/type/attr_type.h"
#include "common/type/date_type.h"
#include "common/value.h"

std::string get_valid_str(const std::string &s) 
{
  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] >= '9' || s[i] <= '0') {
      if (s[i] != '.') {
        return s.substr(0, i);
      }
    }
  }

  return s;
}

// float hexstr_to_float(const std::string& hexStr) 
// {
//     std::string cleanedStr = hexStr;
//     if (cleanedStr.substr(0, 2) == "0x" || cleanedStr.substr(0, 2) == "0X") {
//         cleanedStr = cleanedStr.substr(2);
//     }

//     float result = 0.0;
//     float oper = 1.0 / 16;
//     bool pt_flag = false;
//     for (auto w : cleanedStr) {
//       if (w == '.') {
//         pt_flag = true;
//         continue;
//       }

//       if (w <= '9' && w >= '0') {
//         if (!pt_flag) {
//           result = result * 16 + (w - '0');
//         } else {
//           result = result + (w - '0') * oper;
//           oper /= 16;
//         }
//       } else {
//         if (!pt_flag) {
//           result = result * 16 + ((w >= 'a') ? (w - 'a') : (w - 'A'));
//         } else {
//           result = result + (10 + ((w >= 'a') ? (w - 'a') : (w - 'A'))) * oper;
//           oper /= 16;
//         }
//       }
//     }

//     return result;
// }

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC get_value_from_str(int &val, const string &data) 
{
  std::istringstream date_stream(data);
  char               year_str[5], month_str[3], day_str[3];
  int                year, month, day;

  date_stream >> year_str >> month_str >> day_str;
  year  = atoi(year_str);
  month = atoi(month_str);
  day   = atoi(day_str);

  if (date_stream.fail() || year < 0 || month < 1 || month > 12 || day < 1 || day > get_days_in_month(year, month)) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  int date_int = year * 10000 + month * 100 + day;
  val = date_int;
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  result.set_type(type);

  switch (type) {
    case AttrType::INTS: {
      try { 
        result.set_int(std::stoi(get_valid_str(val.get_string())));
      } catch (std::invalid_argument const &ex) {
        return RC::INVALID_ARGUMENT;
      }
    } break;

    case AttrType::FLOATS: {
      try { 
          result.set_float(std::stof(get_valid_str(val.get_string())));
      } catch (std::invalid_argument const &ex) {
        return RC::INVALID_ARGUMENT;
      }
    } break;

    case AttrType::DATES: {
      int *v = nullptr;
      RC rc = get_value_from_str(*v, val.get_string());
      if (rc != RC::SUCCESS) {
        return rc;
      }
      result.set_date(*v);
    } break;

    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::FLOATS) {
    return 1;
  }
  return 0;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}