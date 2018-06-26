#include <unistd.h>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <random>

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
  value_t* matrix_copy = new value_t[matrix.size()];
  auto     copy_end    = std::copy(matrix.begin(), matrix.end(), matrix_copy);
  DASH_ASSERT(copy_end == matrix_copy + matrix.size());
  cout << "Matrix:" << endl;
  for (auto r = 0; r < rows; ++r) {
    for (auto c = 0; c < cols; ++c) {
      cout << " " << setw(5) << matrix_copy[r * cols + c];
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
    cout << " " << setw(5) << array_copy[r];
  }
  cout << endl;
  delete[] array_copy;
}

template <class GlobIter>
static void rand_range(GlobIter begin, GlobIter end)
{
  using value_t = typename dash::iterator_traits<GlobIter>::value_type;
#ifdef NDEBUG
  dash::fill(begin, end, static_cast<value_t>(begin.pattern().team().myid()));
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

  auto const x_gpos0    = x.pattern().global(0);
  auto const x_gpos_end = x_gpos0 + x.lsize();

  std::vector<dash::Future<value_t*>> futs;

  /*
   * Phase 1: Start Async Copies
   */
  if (x_gpos0 == 0) {
    // first unit
    futs.emplace_back(
        dash::copy_async(x.begin() + x.lsize(), x.end(), xcopy.data()));
  }
  else {
    // last unit
    futs.emplace_back(
        dash::copy_async(x.begin(), x.begin() + x_gpos0, xcopy.data()));

    if (x_gpos_end != x.size()) {
      // if we are in the middle...
      futs.emplace_back(dash::copy_async(
          x.begin() + x_gpos_end,
          x.end(),
          std::next(
              xcopy.data(), dash::distance(x.begin(), x.begin() + x_gpos0))));
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
  if (x_gpos0 == 0) {
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
      for (std::size_t col = 0; col < x_gpos0; ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col);
      }
    }
  }
  else {
    for (std::size_t row = 0; row < nlrows_A; ++row) {
      // first do everything before local part
      for (std::size_t col = 0; col < x_gpos0; ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col);
      }

      // then do everything after local part
      for (std::size_t col = x_gpos_end; col < x.size(); ++col) {
        out.local[row] += A.local[row][col] * xcopy.at(col - nlcols);
      }
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

  using value_t     = double;
  using block_pat_t = dash::BlockPattern<2, dash::ROW_MAJOR>;
  using narray_t =
      dash::NArray<value_t, 2, dash::default_index_t, block_pat_t>;

  auto const myid = dash::myid();

  size_t team_size = dash::Team::All().size();
  size_t extent_x  = dash::size();
  size_t extent_y  = extent_x;

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

  for (std::size_t it = 0; it < 10; ++it) {
    dot(A, x, y);

    // implicit barrier
    auto const norm = vectorNorm(y);

    dot(y, value_t{1 / norm});

    y.barrier();

    if (it < 9) {
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
