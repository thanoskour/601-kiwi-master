#ifndef __KIWI_H_
#define __KIWI_H_

void _write_test(long int count, int r);
void _read_test(long int count, int r);
void _readwrite_test_2_threads(long int count, int r);
void _readwrite_test_X_threads(long int count, int r, int num_of_threads, int read_percentage, int xx);

#endif
