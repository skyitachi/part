//
// Created by skyitachi on 24-1-14.
//
#include "helper.h"

#include <execinfo.h>
#include <fmt/core.h>

#include <memory>

namespace part {

std::string GetStackTrace(int max_depth) {
  std::string result;
  auto callstack = std::unique_ptr<void *[]>(new void *[max_depth]);
  int frames = backtrace(callstack.get(), max_depth);
  char **strs = backtrace_symbols(callstack.get(), frames);
  for (int i = 0; i < frames; i++) {
    result += strs[i];
    result += "\n";
  }
  free(strs);
  return "\n" + result;
}

void PartAssertInternal(bool condition, const char *condition_name, const char *file, int linenr) {
#ifdef DISABLE_ASSERTIONS
  return;
#endif
  if (condition) {
    return;
  }
  try {
    throw TracedException();
  } catch (const TracedException &ex) {
    fmt::println("Assertion triggered in file \"{}\" on line {}: {}", file, linenr, condition_name);
    fmt::println("{}", ex.what());
    throw ex;
  }
}
}  // namespace part
