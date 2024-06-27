//
// Created by skyitachi on 24-5-25.
//
#include <art.h>
#include <string>
#include <time.h>
#include <part/util.h>
#include <functional>

#include "util.h"

using namespace part;

class BenchmarkConfig;

using BenchFn = std::function<void(const BenchmarkConfig*)>;

class BenchmarkConfig {
 public:
  std::string id;
  time_t ts;
  size_t scale;
  size_t n_repeats;
  std::string bench_fn_name;
  BenchFn bench_fn;
};

std::vector<std::pair<ARTKey, idx_t>> make_art_keys(ArenaAllocator &allocator, size_t n_insert, size_t k) {
  int *numbers = make_numbers(n_insert, k);
  shuffle_numbers(numbers, n_insert);

  std::vector<std::pair<ARTKey, idx_t>> art_keys;
  for (size_t i = 0; i < n_insert; i++) {
    ARTKey key = ARTKey::CreateARTKey<int32_t >(allocator, numbers[i]);
    art_keys.emplace_back(key, numbers[i]);
  }

  return art_keys;
}

std::vector<std::pair<ARTKey, idx_t>> make_new_art_keys(ArenaAllocator &allocator, size_t n, int *numbers) {
  std::vector<std::pair<ARTKey, idx_t>> art_keys;
  for (size_t i = 0; i < n; i++) {
    ARTKey key = ARTKey::CreateARTKey<int32_t >(allocator, numbers[i]);
    art_keys.emplace_back(key, numbers[i]);
  }

  return art_keys;
}

static void perf_inset(const BenchmarkConfig *cfg) {

  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  auto art_keys = make_art_keys(arena_allocator, cfg->scale, 0);

  int n_insert = 0.1 * cfg->scale;

  int *new_numbers = make_numbers(n_insert, cfg->scale);

  auto new_art_keys = make_art_keys(arena_allocator, n_insert, cfg->scale);

  TimeInterval ti_insert;
  for (size_t i =0 ; i < cfg->n_repeats; i++) {
    ART art;
    for (size_t j = 0; j < cfg->scale; j++) {
      art.Put(art_keys[j].first, art_keys[j].second);
    }

    shuffle_numbers(new_numbers, n_insert);

    auto insert_art_keys = make_new_art_keys(arena_allocator, n_insert, new_numbers);
    timer_start(&ti_insert);
    for (size_t j = 0; j < n_insert; j++) {
      art.Put(insert_art_keys[j].first, insert_art_keys[j].second);
    }
    timer_stop(&ti_insert);

    double ns_per_insert = timer_nsec(&ti_insert) / (double)n_insert;
    printf("%ld,\"%s\",%lu,\"%s\",%lu,%f\n", cfg->ts, cfg->id.c_str(), i, "insert",
           cfg->scale, ns_per_insert);

  }
}

static void perf_query(const BenchmarkConfig* cfg) {

}

int main() {
  size_t scale[] = {1000, 10000, 100000, 1000000};
  size_t n_scales = 4;
  std::vector<std::unique_ptr<BenchmarkConfig>> benches;

  std::vector<BenchFn> bench_fns = {perf_inset};

  for (size_t i = 0; i < n_scales; i++) {
    auto cfg = std::make_unique<BenchmarkConfig>();
    cfg->scale = scale[i];
    cfg->n_repeats = 20;
    cfg->id = "art_insert";
    cfg->ts = time(0);
    cfg->bench_fn = perf_inset;
    cfg->bench_fn(cfg.get());
  }
}