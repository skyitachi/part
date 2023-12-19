//
// Created by Shiping Yao on 2023/12/19.
//
#include <benchmark/benchmark.h>
#include <chrono>
#include <filesystem>

#include "art.h"
#include "util.h"
#include "arena_allocator.h"


namespace bm = benchmark;
using namespace part;

using high_resolution_clock_t = std::chrono::high_resolution_clock;
using time_point_t = std::chrono::time_point<high_resolution_clock_t>;
using elapsed_time_t = std::chrono::nanoseconds;
using microseconds_t = std::chrono::duration<long long, std::micro>;

class ARTTestFixture : public benchmark::Fixture {
public:
  void SetUp(const benchmark::State &state) override {
    printf("SetUp\n");
  }

  void TearDown(const benchmark::State &state) override {
    printf("TearDown\n");
  }

  int add(int a, int b) {
    return a + b;
  }
};

int add(int a, int b) {
  return a + b;
}

template <typename func_at>
inline void register_benchmark(std::string const& name, size_t threads_count, func_at func) {
  bm::RegisterBenchmark(name.c_str(), func)
      ->Threads(threads_count)
      ->Unit(bm::kMicrosecond)
      ->UseRealTime()
      ->Repetitions(1)
      ->Iterations(1);
}

struct ARTKeyHash {
  std::size_t operator()(const ARTKey &key) const {
    return std::hash<int64_t>{}(*reinterpret_cast<int64_t*>(key.data));
  }
};

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  Random random;
  ART art;
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  int limit = 100000;
  auto kv_pairs = random.GenKvPairs(limit);

  register_benchmark("art_unordered_write_test", 1, [&](bm::State &st) {
    while (st.KeepRunning()) {
      for(int i = 0; i < 100000; i++) {
        art.Put(kv_pairs[i].first, kv_pairs[i].second);
      }
    }
  });

  ART art2("benchmark_i64.meta", "benchmark_i64.data");

  register_benchmark("art_unordered_write_serialize_test", 1, [&](bm::State &st) {
    while (st.KeepRunning()) {
      for(int i = 0; i < 100000; i++) {
        art2.Put(kv_pairs[i].first, kv_pairs[i].second);
      }
      art2.Serialize();
    }
    std::filesystem::remove("benchmark_i64.meta");
    std::filesystem::remove("benchmark_i64.data");
  });

  register_benchmark("std_unordered_map_insert", 1, [&](bm::State &st) {
    std::unordered_map<ARTKey, int64_t, ARTKeyHash> map;
    while (st.KeepRunning()) {
      for(int i = 0; i < 100000; i++) {
        map.insert(kv_pairs[i]);
      }
    }
  });

  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
