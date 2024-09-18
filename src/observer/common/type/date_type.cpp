#include "date_type.h"
#include "common/log/log.h"
#include "common/value.h"
#include <sstream>
#include <iomanip>

// bool check_date(int year, int month, int day)
// {
//   static int mon[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
//   LOG_WARN("check_date: year %d,month %d,day %d");
//   bool leap = (year % 400 == 0 || (year % 100 && year % 4 == 0));
//   if (year > 0 && (month > 0) && (month <= 12) && (day > 0) && (day <= ((month == 2 && leap) ? 1 : 0) + mon[month]))
//     return true;
//   else
//     return false;
// }

// int string_to_date(const std::string &str, int32_t &date)
// {
//   int y, m, d;
//   sscanf(str.c_str(), "%d-%d-%d", &y, &m, &d);  // not check return value eq 3, lex guarantee
//   bool b = check_date(y, m, d);
//   if (!b)
//     return -1;
//   date = y * 10000 + m * 100 + d;
//   return 0;
// }

// std::string date_to_string(int32_t date)
// {
//   std::string ans       = "YYYY-MM-DD";
//   std::string tmp       = std::to_string(date);
//   int         tmp_index = 7;
//   for (int i = 9; i >= 0; i--) {
//     if (i == 7 || i == 4) {
//       ans[i] = '-';
//     } else {
//       ans[i] = tmp[tmp_index--];
//     }
//   }
//   return ans;
// }

// Helper function to check if a year is a leap year
bool is_leap_year(int year) { return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0); }

// Helper function to get the number of days in a month
int get_days_in_month(int year, int month)
{
  int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month == 2 && is_leap_year(year)) {
    return 29;
  }
  return days_in_month[month - 1];
}

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES && right.attr_type() == AttrType::DATES, "Invalid type");
  int left_days  = left.get_int();
  int right_days = right.get_int();
  return (left_days > right_days) ? 1 : (left_days < right_days ? -1 : 0);
}

RC DateType::add(const Value &left, const Value &right, Value &result) const
{
  // Implement date addition logic here
  return RC::UNSUPPORTED;
}

RC DateType::subtract(const Value &left, const Value &right, Value &result) const
{
  // Implement date subtraction logic here
  return RC::UNSUPPORTED;
}

RC DateType::multiply(const Value &left, const Value &right, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::divide(const Value &left, const Value &right, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::negative(const Value &val, Value &result) const { return RC::UNSUPPORTED; }

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  result.set_type(type);

  switch (type) {
    case AttrType::INTS: {
      result.set_int(val.get_int());
    } break;

    case AttrType::FLOATS: {
      result.set_float(val.get_int());
    } break;

    case AttrType::CHARS: {
      result.set_string_from_other(val);
    } break;

    case AttrType::DATES: {
      result.set_date((int)val.get_int());
    } break;

    default:
      return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
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
  val.set_int(date_int);
  return RC::SUCCESS;
}

RC DateType::to_string(const Value &val, string &result) const
{
  int               date_int = val.get_int();
  std::stringstream ss;
  ss << std::setw(4) << std::setfill('0') << date_int / 10000 << "-" << std::setw(2) << std::setfill('0')
     << (date_int / 100 % 100) << "-" << std::setw(2) << std::setfill('0') << (date_int % 100);
  result = ss.str();
  return RC::SUCCESS;
}