#include <unistd.h>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <random>

#include <numeric>
#include <dash/LocalMirror.h>
#include <libdash.h>

using std::cout;
using std::endl;
using std::setw;

template <class MatrixT>
void print_matrix(const MatrixT& matrix)
{
  typedef typename MatrixT::value_type value_t;
  auto                                 rows = matrix.extent(0);
  auto                                 cols = matrix.extent(1);

  // Creating local copy for output to prevent interleaving with log
  // messages:
  value_t*             matrix_copy = new value_t[matrix.size()];
  std::vector<value_t> lcopy(matrix.size());
  auto copy_end = std::copy(matrix.begin(), matrix.end(), std::begin(lcopy));
  DASH_ASSERT(copy_end == std::end(lcopy));
  cout << "Matrix:" << endl;
  for (auto r = 0; r < rows; ++r) {
    for (auto c = 0; c < cols; ++c) {
      cout << " " << setw(5) << std::setprecision(3) << lcopy[r * cols + c];
    }
    cout << endl;
  }
  delete[] matrix_copy;
}

template <class ArrayT>
void print_array(const ArrayT& array)
{
  typedef typename ArrayT::value_type value_t;

  // Creating local copy for output to prevent interleaving with log
  // messages:
  value_t* array_copy = new value_t[array.size()];
  auto     copy_end   = dash::copy(array.begin(), array.end(), array_copy);
  DASH_ASSERT(copy_end == array_copy + array.size());
  cout << "Array:" << endl;
  for (auto r = 0; r < array.size(); ++r) {
    cout << " " << setw(5) << std::setprecision(3) << array_copy[r];
  }
  cout << endl;
  delete[] array_copy;
}

template <class GlobIter>
static void rand_range(GlobIter begin, GlobIter end)
{
  using value_t = typename dash::iterator_traits<GlobIter>::value_type;
#ifndef NDEBUG
  dash::fill(
      begin,
      end,
      dash::size() > 1 ? static_cast<value_t>(begin.pattern().team().myid())
                       : 1);
#else

  static std::uniform_real_distribution<> distribution(-10.0f, 10.0f);
  static std::random_device               rd;
  static std::mt19937 generator(rd() + begin.team().myid());

  dash::generate(begin, end, []() { return distribution(generator); });
#endif
}

template <class MatrixT, class ArrayT>
static void dot(MatrixT const& A, ArrayT const& x, ArrayT& out)
{
  using iterator_t = decltype(A.begin());

  using value_t = typename std::remove_cv<
      typename dash::iterator_traits<iterator_t>::value_type>::type;

  assert(x.lsize() == out.lsize());

  auto const nlrows_A = A.local.extent(0);
  auto const nlcols_A = A.local.extent(1);
  auto const nlrows_x = x.lsize();

  assert(A.extent(1) == x.size());

  std::vector<value_t> xcopy(x.size() - x.lsize());

  auto const x_gpos_begin = x.pattern().global(0);
  auto const x_gpos_end   = x_gpos_begin + x.lsize();

  std::vector<dash::Future<value_t*>> futs;

  /*
   * Phase 1: Start Async Copies
   */
  if (x_gpos_begin == 0) {
    // first unit
    futs.emplace_back(
        dash::copy_async(x.begin() + x.lsize(), x.end(), xcopy.data()));
  }
  else {
    // last unit
    futs.emplace_back(
        dash::copy_async(x.begin(), x.begin() + x_gpos_begin, xcopy.data()));

    if (x_gpos_end != x.size()) {
      // if we are in the middle...
      futs.emplace_back(dash::copy_async(
          x.begin() + x_gpos_end,
          x.end(),
          std::next(
              xcopy.data(),
              dash::distance(x.begin(), x.begin() + x_gpos_begin))));
    }
  }

  /*
   * Phase 2: Compute local part
   */
  std::fill(out.lbegin(), out.lend(), 0);

  auto const nlcols = std::min(nlcols_A, nlrows_x);

  for (std::size_t row = 0; row < nlrows_A; ++row) {
    for (std::size_t col = 0; col < nlcols; ++col) {
      out.local[row] += A.local[row][col] * x.local[col];
    }
  }

  /*
   * Phase 3: Wait for Async Copies
   */
  for (auto& fut : futs) {
    fut.wait();
  }

  /*
   * Phase 4: Compute other parts with remote values
   */
  if (x_gpos_begin == 0) {
    for (std::size_t row = 0; row < nlrows_A; ++row) {
      // we start now with the first non-local column
      for (std::size_t col = x_gpos_end; col < x.size(); ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col - nlcols);
      }
    }
  }
  else if (x_gpos_end == x.size()) {
    for (std::size_t row = 0; row < nlrows_A; ++row) {
      // we start now with the first non-local column
      for (std::size_t col = 0; col < x_gpos_begin; ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col);
      }
    }
  }
  else {
    for (std::size_t row = 0; row < nlrows_A; ++row) {
      // first do everything before local part
      for (std::size_t col = 0; col < x_gpos_begin; ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col);
      }

      // then do everything after local part
      for (std::size_t col = x_gpos_end; col < x.size(); ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col - nlcols);
      }
    }
  }
}

template <class MatrixT, class ArrayT>
static void dot_full_cpy(MatrixT const& A, ArrayT const& x, ArrayT& out)
{
  using iterator_t = decltype(A.begin());

  using value_t = typename std::remove_cv<
      typename dash::iterator_traits<iterator_t>::value_type>::type;

  DASH_ASSERT_ALWAYS(x.lsize() == out.lsize());

  constexpr size_t DIMX = 1;
  constexpr size_t DIMY = 0;

  DASH_ASSERT_ALWAYS(A.extent(DIMY) == x.size());

  std::vector<value_t> xcopy(x.size());

  auto const x_gpos_begin = x.pattern().global(0);
  auto const x_gpos_end   = x_gpos_begin + x.lsize();

  std::vector<dash::Future<value_t*>> futs;

  // copy everything before local part...
  futs.emplace_back(
      dash::copy_async(x.begin(), x.begin() + x_gpos_begin, xcopy.data()));

  // copy everything after local part...
  futs.emplace_back(dash::copy_async(
      x.begin() + x_gpos_end, x.end(), std::next(xcopy.data(), x_gpos_end)));

  /*
   * Phase 2: Compute local part
   */
  std::fill(out.lbegin(), out.lend(), 0);

  // jump over locals rows and cols of A and multiply it with x.local
  for (std::size_t row = 0; row < A.local.extent(DIMY); ++row) {
    for (std::size_t col = 0; col < x.lsize(); ++col) {
      out.local[row] += A.local[row][col] * x.local[col];
    }
  }

  // copy local -> give more time to complete outstanding requests
  std::copy(x.lbegin(), x.lend(), std::next(xcopy.data(), x_gpos_begin));

  /*
   * Phase 3: Wait for Async Copies
   */
  for (auto& fut : futs) {
    fut.wait();
  }

  /*
   * Phase 4: Compute other parts with remote values
   */
  for (std::size_t row = 0; row < A.local.extent(DIMY); ++row) {
    // everything before local part of x
    for (std::size_t col = x_gpos_end; col < x.size(); ++col) {
      out.local[row] += A.local[row][col] * xcopy.at(col);
    }

    // everything after local part of x
    for (std::size_t col = 0; col < x_gpos_begin; ++col) {
      out.local[row] += A.local[row][col] * xcopy.at(col);
    }
  }
}

template <class MatrixT, class ArrayT>
static void dot_mirror(MatrixT const& A, ArrayT const& x, ArrayT& out)
{
  using iterator_t = decltype(A.begin());
  using value_t    = typename dash::iterator_traits<iterator_t>::value_type;

  DASH_ASSERT_ALWAYS(x.lsize() == out.lsize());

  constexpr size_t DIMX = 1;
  constexpr size_t DIMY = 0;

  DASH_ASSERT_ALWAYS(A.extent(DIMY) == x.size());

#if 1
  using mirror_t = dash::LocalMirror<decltype(x.begin()), dash::HostSpace>;
#else
  using mirror_t = dash::LocalMirror<decltype(x.begin()), dash::HBWSpace>;
#endif

  mirror_t mirror{/* reduction_operator, permissions, monitoring */};

  //mirror.pull -> launch policy
  //mirror.push
  //mirror.pull_local ?
  //mirror.push_local ?
  //-> return future from replication / copy
  //-> launch policy -> sync vs. async
  mirror.replicate(x.begin(), x.end());

  //mirror.wait_local() -> local data replicated

  /*
   * Phase 2: Compute local part
   */
  std::fill(out.lbegin(), out.lend(), 0);

  // jump over locals rows and cols of A and multiply it with x.local
  for (std::size_t row = 0; row < A.local.extent(DIMY); ++row) {
    for (std::size_t col = 0; col < x.lsize(); ++col) {
      auto const val = mirror.lbegin()[col];
      out.local[row] += A.local[row][col] * mirror.lbegin()[col];
    }
  }

  auto const x_gpos_begin = x.pattern().lbegin();
  auto const x_gpos_end   = x.pattern().lend();

  //mirror,flush() -> remote data is available on return

  /*
   * Phase 4: Compute other parts with remote values
   */
  for (std::size_t row = 0; row < A.local.extent(DIMY); ++row) {
    // everything before local part of x
    for (std::size_t col = x_gpos_end; col < x.size(); ++col) {
      out.local[row] += A.local[row][col] * mirror.begin()[col];
    }

    // everything after local part of x
    for (std::size_t col = 0; col < x_gpos_begin; ++col) {
      out.local[row] += A.local[row][col] * mirror.begin()[col];
    }
  }
}

template <class ArrayT>
double vectorNorm(const ArrayT& array)
{
  using value_t = typename ArrayT::value_type;

  value_t gresult{0};

  auto const lresult = std::accumulate(
      array.local.begin(),
      array.local.end(),
      value_t{0},
      [](const value_t& a, const value_t& b) { return a + (b * b); });

  dart_allreduce(
      &lresult,
      &gresult,
      1,
      dash::dart_datatype<value_t>::value,
      DART_OP_SUM,
      array.team().dart_id());

  return std::sqrt(gresult);
}

template <class ArrayT>
void dot(ArrayT& inout, typename ArrayT::value_type scalar)
{
  for (std::size_t idx = 0; idx < inout.lsize(); ++idx) {
    inout.local[idx] = inout.local[idx] * scalar;
  }
}

int main(int argc, char* argv[])
{
  dash::init(&argc, &argv);

  int wait = 1;
  while(wait);

  using value_t     = double;
  using block_pat_t = dash::BlockPattern<2, dash::ROW_MAJOR>;
  using narray_t =
      dash::NArray<value_t, 2, dash::default_index_t, block_pat_t>;

  auto const myid = dash::myid();

  constexpr size_t niter = 10;

  size_t team_size = dash::Team::All().size();
  size_t extent_x  = 4;
  size_t extent_y  = 4;

  block_pat_t pat_blocked_row{
      dash::SizeSpec<2>(extent_y, extent_x),
      dash::DistributionSpec<2>(dash::BLOCKED, dash::NONE),
      dash::TeamSpec<2>(dash::Team::All()),
      dash::Team::All()};

  narray_t A{pat_blocked_row};

  dash::Array<value_t> x(A.extent(1));
  dash::Array<value_t> y(A.extent(1));

  rand_range(A.begin(), A.end());
  rand_range(x.begin(), x.end());

  A.barrier();

  if (0 == myid) {
    print_matrix(A);
    print_array(x);
  }

  for (std::size_t it = 0; it < niter; ++it) {
    // dot(A, x, y);
    // dot_full_cpy(A, x, y);
    dot_mirror(A, x, y);

    // implicit barrier
    auto const norm = vectorNorm(y);

    dot(y, value_t{1 / norm});

    y.barrier();

    y.barrier();

    if (it < niter - 1) {
      std::swap(x, y);
    }

    y.barrier();
  }

  if (dash::myid() == 0) {
    print_array(y);
  }

  dash::barrier();

  dash::finalize();
}
