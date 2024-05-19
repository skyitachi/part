//
// Created by skyitachi on 24-5-12.
//

#ifndef PART_UTIL_H
#define PART_UTIL_H

#include <stddef.h>
#include <stdio.h>
#include <time.h>

int *make_numbers(const size_t n, const size_t k);
int *shuffle_numbers(int *arr, size_t size);


struct TimeInterval {
  struct timespec begin, end;
  time_t sec;
  long nsec;
};

void timer_start(struct TimeInterval *ti);
void timer_stop(struct TimeInterval *ti);
void timer_continue(struct TimeInterval *ti);
long timer_nsec(struct TimeInterval *ti);
void print_timer(struct TimeInterval *ti, const time_t timestamp,
                 const char *benchmark_id, size_t ix, const char *tag);


#endif  // PART_UTIL_H
