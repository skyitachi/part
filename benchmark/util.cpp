//
// Created by skyitachi on 24-5-12.
//
#include <stdlib.h>
#include "util.h"

/*
 * Create an integer sequence k, k+1, ..., k + n.
 */
int *make_numbers(const size_t n, const size_t k)
{
  int *numbers = (int *)malloc(n * sizeof(int));
  if (numbers) {
    for (int i = 0; i < n; ++i)
      numbers[i] = k + i;
  }
  return numbers;
}

/*
 * Shuffle numbers in-place.
 */
int *shuffle_numbers(int *arr, size_t size)
{
  int tmp;
  for (size_t i = 0; i < size - 1; ++i) {
    size_t j = drand48() * (i + 1);
    tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
  return arr;
}



void timer_start(struct TimeInterval *ti)
{
  ti->sec = 0;
  ti->nsec = 0;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ti->begin);
}

void timer_stop(struct TimeInterval *ti)
{
  clock_gettime(CLOCK_MONOTONIC_RAW, &ti->end);
  ti->sec += ti->end.tv_sec - ti->begin.tv_sec;
  ti->nsec += ti->end.tv_nsec - ti->begin.tv_nsec;
}

void timer_continue(struct TimeInterval *ti)
{
  clock_gettime(CLOCK_MONOTONIC_RAW, &ti->begin);
}

long timer_nsec(struct TimeInterval *ti)
{
  return ti->sec * 1000000000L + ti->nsec;
}

void print_timer(struct TimeInterval *ti, const time_t timestamp,
                 const char *benchmark_id, size_t ix, const char *tag)
{
  printf("%ld, %s, %lu, %s,%lu\n", timestamp, benchmark_id, ix, tag,
         ti->sec * 1000000000L + ti->nsec);
}