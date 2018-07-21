#pragma once

#include <string>

namespace sphinx {

inline
std::string to_string(unsigned long n)
{
  if (n != 0) {
    constexpr auto size = 20;
    char ret[size];
    size_t offset = size;
    while (n > 0) {
      auto digit = n % 10;
      n = (n - digit) / 10;
      ret[--offset] = '0' + digit;
    }
    return std::string{ret + offset, size - offset};
  } else {
    return "0";
  }
}
}
