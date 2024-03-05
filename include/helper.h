//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_HELPER_H
#define PART_HELPER_H
#include <backward.hpp>
#include <string>

namespace part {
template <class T, T val = 8>
static inline T AlignValue(T n) {
  return ((n + (val - 1)) / val) * val;
}

class TracedException : public std::runtime_error {
 public:
  TracedException() : std::runtime_error(_get_trace()) {}

 private:
  std::string _get_trace() {
    std::ostringstream ss;

    backward::StackTrace stackTrace;
    backward::TraceResolver resolver;
    stackTrace.load_here();
    resolver.load_stacktrace(stackTrace);

    for (std::size_t i = 0; i < stackTrace.size(); ++i) {
      const backward::ResolvedTrace trace = resolver.resolve(stackTrace[i]);

      ss << "#" << i << " at " << trace.object_function << "\n";
    }

    return ss.str();
  }
};

std::string GetStackTrace(int max_depth = 120);

void PartAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);

#define P_ASSERT(condition) part::PartAssertInternal(bool(condition), #condition, __FILE__, __LINE__)

}  // namespace part
#endif  // PART_HELPER_H
