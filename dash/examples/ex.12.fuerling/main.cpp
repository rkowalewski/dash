#include <unistd.h>
#include <iostream>
#include <cstddef>
#include <sstream>

#include <libdash.h>

using namespace std;

#include <sys/time.h>
#include <time.h>

#define MYTIMEVAL( tv_ )                        \
  ((tv_.tv_sec)+(tv_.tv_usec)*1.0e-6)

#define TIMESTAMP( time_ )                                              \
  {                                                                     \
    static struct timeval tv;                                           \
    gettimeofday( &tv, NULL );                                          \
    time_=MYTIMEVAL(tv);                                                \
  }

//
// do some work and measure how long it takes
//
double do_work(int *beg, int nelem, int repeat)
{
  const int LCG_A = 1664525, LCG_C = 1013904223;

  int seed = 31337;
  double start, end;

  TIMESTAMP(start);
  for( int j=0; j<repeat; j++ ) {
    for( int i=0; i<nelem; ++i ) {
      seed = LCG_A * seed + LCG_C;
      beg[i] = ((unsigned)seed) %100;
    }
  }
  TIMESTAMP(end);

  return end-start;
}

int main(int argc, char* argv[])
{
  dash::init(&argc, &argv);

  dash::Array<int> arr(100000000);

  int nelem = arr.local.size();

  int *mem = (int*) malloc(sizeof(int)*nelem);

  double dur1 = do_work(arr.lbegin(), nelem, 1);
  double dur2 = do_work(mem,          nelem, 1);

  cerr << "Unit " << dash::myid()
       << " DASH mem: " << dur1 << " secs"
       << " Local mem: " << dur2 << " secs" << endl;

  dash::finalize();

  return EXIT_SUCCESS;
}
