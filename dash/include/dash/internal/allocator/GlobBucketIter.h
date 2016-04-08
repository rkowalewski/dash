#ifndef DASH__INTERNAL__ALLOCATOR__GLOB_BUCKET_ITER_H__INCLUDED
#define DASH__INTERNAL__ALLOCATOR__GLOB_BUCKET_ITER_H__INCLUDED

#include <dash/dart/if/dart.h>

#include <dash/Types.h>
#include <dash/GlobPtr.h>
#include <dash/Allocator.h>
#include <dash/Team.h>
#include <dash/Onesided.h>

#include <dash/internal/Logging.h>
#include <dash/internal/allocator/GlobDynamicMemTypes.h>
#include <dash/internal/allocator/LocalBucketIter.h>

#include <type_traits>
#include <list>
#include <vector>
#include <iterator>
#include <sstream>
#include <iostream>


namespace dash {

// Forward-declaration
template<
  typename ElementType,
  class    AllocatorType >
class GlobDynamicMem;

namespace internal {

/**
 * Iterator on global buckets. Represents global pointer type.
 */
template<
  typename ElementType,
  class    GlobMemType,
  class    PointerType   = dash::GlobPtr<ElementType>,
  class    ReferenceType = dash::GlobRef<ElementType> >
class GlobBucketIter
: public std::iterator<
           std::random_access_iterator_tag,
           ElementType,
           typename GlobMemType::index_type,
           PointerType,
           ReferenceType >
{
private:
  typedef GlobBucketIter<
            ElementType,
            GlobMemType,
            PointerType,
            ReferenceType>
    self_t;

public:
  typedef typename GlobMemType::index_type                       index_type;
  typedef typename std::make_unsigned<index_type>::type           size_type;

  typedef       ElementType                                      value_type;
  typedef       ReferenceType                                     reference;
  typedef const ReferenceType                               const_reference;
  typedef       PointerType                                         pointer;
  typedef const PointerType                                   const_pointer;

  typedef typename
    std::conditional<
      std::is_const<value_type>::value,
      typename GlobMemType::const_local_pointer,
      typename GlobMemType::local_pointer
    >::type
    local_pointer;

  typedef struct {
    dart_unit_t unit;
    index_type  index;
  } local_index;

private:
  typedef std::vector<std::vector<size_type> >
    bucket_cumul_sizes_map;

public:
  /**
   * Default constructor.
   */
  GlobBucketIter()
  : _globmem(nullptr),
    _bucket_cumul_sizes(nullptr),
    _idx(0),
    _max_idx(0),
    _myid(dash::myid()),
    _idx_unit_id(DART_UNDEFINED_UNIT_ID),
    _idx_local_idx(-1),
    _idx_bucket_idx(-1),
    _idx_bucket_phase(-1)
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter()", _idx);
    DASH_LOG_TRACE_VAR("GlobBucketIter()", _max_idx);
  }

  /**
   * Constructor, creates a global iterator on global memory from global
   * offset in logical storage order.
   */
  GlobBucketIter(
    GlobMemType * gmem,
	  index_type    position = 0)
  : _globmem(gmem),
    _bucket_cumul_sizes(&_globmem->_bucket_cumul_sizes),
    _lbegin(_globmem->lbegin()),
    _idx(position),
    _max_idx(gmem->size() - 1),
    _myid(dash::myid()),
    _idx_unit_id(0),
    _idx_local_idx(0),
    _idx_bucket_idx(0),
    _idx_bucket_phase(0)
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter(gmem,idx)", position);
    for (auto unit_bucket_cumul_sizes : *_bucket_cumul_sizes) {
      _idx_local_idx    = position;
      _idx_bucket_phase = position;
      for (auto bucket_cumul_size : unit_bucket_cumul_sizes) {
        if (position > bucket_cumul_size) {
          _idx_bucket_phase -= bucket_cumul_size;
          break;
        }
        ++_idx_bucket_idx;
      }
      // advance to next unit, adjust position relative to next unit's
      // local index space:
      position -= unit_bucket_cumul_sizes.back();
      ++_idx_unit_id;
    }
    DASH_LOG_TRACE("GlobBucketIter(gmem,idx)",
                   "gidx:",   _idx,
                   "unit:",   _idx_unit_id,
                   "lidx:",   _idx_local_idx,
                   "bucket:", _idx_bucket_idx,
                   "phase:",  _idx_bucket_phase);
  }

  /**
   * Constructor, creates a global iterator on global memory from unit and
   * local offset in logical storage order.
   */
  GlobBucketIter(
    GlobMemType * gmem,
    dart_unit_t   unit,
	  index_type    local_index)
  : _globmem(gmem),
    _bucket_cumul_sizes(&_globmem->_bucket_cumul_sizes),
    _lbegin(_globmem->lbegin()),
    _idx(0),
    _max_idx(gmem->size() - 1),
    _myid(dash::myid()),
    _idx_unit_id(unit),
    _idx_local_idx(local_index),
    _idx_bucket_idx(0),
    _idx_bucket_phase(0)
  {
    DASH_LOG_TRACE("GlobBucketIter(gmem,unit,lidx)", unit, local_index);
    DASH_ASSERT_LT(unit, _bucket_cumul_sizes->size(), "invalid unit id");

    for (size_type unit = 0; unit < _idx_unit_id; ++unit) {
      auto prec_unit_local_size = (*_bucket_cumul_sizes)[unit].back();
      _idx += prec_unit_local_size;
    }
    _idx_bucket_phase = _idx_local_idx;
    for (auto unit_bucket_cumul_size : (*_bucket_cumul_sizes)[_idx_unit_id])
    {
      if (_idx_local_idx < unit_bucket_cumul_size) {
        break;
      }
      _idx_bucket_phase -= unit_bucket_cumul_size;
      _idx_bucket_idx++;
    }
    _idx += _idx_local_idx;
    DASH_LOG_TRACE("GlobBucketIter(gmem,unit,lidx)",
                   "gidx:",   _idx,
                   "unit:",   _idx_unit_id,
                   "lidx:",   _idx_local_idx,
                   "bucket:", _idx_bucket_idx,
                   "phase:",  _idx_bucket_phase);
  }

  /**
   * Copy constructor.
   */
  GlobBucketIter(
    const self_t & other) = default;

  /**
   * Assignment operator.
   */
  self_t & operator=(
    const self_t & other) = default;

  /**
   * Type conversion operator to \c GlobPtr.
   *
   * \return  A global reference to the element at the iterator's position
   */
  operator PointerType() const
  {
    return PointerType(dart_gptr());
  }

  /**
   * Explicit conversion to \c dart_gptr_t.
   *
   * \return  A DART global pointer to the element at the iterator's
   *          position
   */
  dart_gptr_t dart_gptr() const
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter.dart_gptr()", _idx);
    index_type idx    = _idx;
    index_type offset = 0;
    // Convert iterator position (_idx) to local index and unit.
    if (_idx > _max_idx) {
      // Global iterator pointing past the range indexed by the pattern
      // which is the case for .end() iterators.
      idx     = _max_idx;
      offset += _idx - _max_idx;
      DASH_LOG_TRACE_VAR("GlobBucketIter.dart_gptr", _max_idx);
      DASH_LOG_TRACE_VAR("GlobBucketIter.dart_gptr", idx);
      DASH_LOG_TRACE_VAR("GlobBucketIter.dart_gptr", offset);
    }
    // Create global pointer from unit, bucket and phase:
    dart_gptr_t dart_gptr = _globmem->dart_gptr_at(
                              _idx_unit_id,
                              _idx_bucket_idx,
                              _idx_bucket_phase + offset);
    DASH_LOG_TRACE_VAR("GlobBucketIter.dart_gptr >", dart_gptr);
    return dart_gptr;
  }

  /**
   * Dereference operator.
   *
   * \return  A global reference to the element at the iterator's position.
   */
  reference operator*() const
  {
    return reference(dart_gptr());
  }

  /**
   * Subscript operator, returns global reference to element at given
   * global index.
   */
  reference operator[](
    /// The global position of the element
    index_type g_index) const
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter.[]()", g_index);
    auto gbit = *this;
    gbit += g_index;
    auto gref = *gbit;
    DASH_LOG_TRACE_VAR("GlobBucketIter.[] >", gref);
  }

  /**
   * Checks whether the element referenced by this global iterator is in
   * the calling unit's local memory.
   */
  inline bool is_local() const
  {
    return (_myid == _idx_unit_id);
  }

  /**
   * Conversion to local bucket iterator.
   *
   * TODO
   */
  local_pointer local() const
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter.local()", _idx);
    index_type idx    = _idx;
    index_type offset = 0;
    DASH_LOG_TRACE_VAR("GlobBucketIter.local", _max_idx);
    // Convert iterator position (_idx) to local index and unit.
    if (_idx > _max_idx) {
      // Global iterator pointing past the range indexed by the pattern
      // which is the case for .end() iterators.
      idx     = _max_idx;
      offset += _idx - _max_idx;
    }
    DASH_LOG_TRACE_VAR("GlobBucketIter.local", idx);
    DASH_LOG_TRACE_VAR("GlobBucketIter.local", offset);
    // Global index to local index and unit:
    auto l_idx = _idx_local_idx + offset;
    DASH_LOG_TRACE_VAR("GlobBucketIter.local >", _idx_unit_id);
    DASH_LOG_TRACE_VAR("GlobBucketIter.local >", l_idx);
    if (_myid != _idx_unit_id) {
      // Iterator position does not point to local element
      return nullptr;
    }
    return (_lbegin + l_idx);
  }

  /**
   * Unit and local offset at the iterator's position.
   *
   * TODO
   */
  inline local_index lpos() const
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter.lpos()", _idx);
    index_type idx    = _idx;
    index_type offset = 0;
    local_index local_pos;
    // Convert iterator position (_idx) to local index and unit.
    if (_idx > _max_idx) {
      // Global iterator pointing past the range indexed by the pattern
      // which is the case for .end() iterators.
      idx    = _max_idx;
      offset = _idx - _max_idx;
      DASH_LOG_TRACE_VAR("GlobBucketIter.lpos", _max_idx);
      DASH_LOG_TRACE_VAR("GlobBucketIter.lpos", idx);
      DASH_LOG_TRACE_VAR("GlobBucketIter.lpos", offset);
    }
    // Global index to local index and unit:
    local_pos.unit  = _idx_unit_id;
    local_pos.index = _idx_local_idx + offset;
    return local_pos;
  }

  /**
   * Map iterator to global index domain.
   */
  inline self_t global() const
  {
    return *this;
  }

  /**
   * Position of the iterator in global index space.
   */
  inline index_type pos() const
  {
    return _idx;
  }

  /**
   * Position of the iterator in global index range.
   */
  inline index_type gpos() const
  {
    return _idx;
  }

  /**
   * The instance of \c GlobMem used by this iterator to resolve addresses
   * in global memory.
   */
  inline const GlobMemType & globmem() const
  {
    return *_globmem;
  }

  /**
   * The instance of \c GlobMem used by this iterator to resolve addresses
   * in global memory.
   */
  inline GlobMemType & globmem()
  {
    return *_globmem;
  }

  /**
   * Prefix increment operator.
   */
  inline self_t & operator++()
  {
    increment(1);
    return *this;
  }

  /**
   * Prefix decrement operator.
   */
  inline self_t & operator--()
  {
    decrement(1);
    return *this;
  }

  /**
   * Postfix increment operator.
   */
  inline self_t operator++(int)
  {
    auto result = *this;
    increment(1);
    return result;
  }

  /**
   * Postfix decrement operator.
   */
  inline self_t operator--(int)
  {
    auto result = *this;
    decrement(1);
    return result;
  }

  inline self_t & operator+=(index_type offset)
  {
    increment(offset);
    return *this;
  }

  inline self_t & operator-=(index_type offset)
  {
    increment(offset);
    return *this;
  }

  inline self_t operator+(index_type offset) const
  {
    auto res = *this;
    increment(offset);
    return res;
  }

  inline self_t operator-(index_type offset) const
  {
    auto res = *this;
    decrement(offset);
    return res;
  }

  inline index_type operator+(
    const self_t & other) const
  {
    return _idx + other._idx;
  }

  inline index_type operator-(
    const self_t & other) const
  {
    return _idx - other._idx;
  }

  inline bool operator<(const self_t & other) const
  {
    return (_idx < other._idx);
  }

  inline bool operator<=(const self_t & other) const
  {
    return (_idx <= other._idx);
  }

  inline bool operator>(const self_t & other) const
  {
    return (_idx > other._idx);
  }

  inline bool operator>=(const self_t & other) const
  {
    return (_idx >= other._idx);
  }

  inline bool operator==(const self_t & other) const
  {
    return _idx == other._idx;
  }

  inline bool operator!=(const self_t & other) const
  {
    return _idx != other._idx;
  }

private:
  void increment(int offset)
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter.increment", offset);
    _idx += offset;
    if (_idx_local_idx + offset <
        (*_bucket_cumul_sizes)[_idx_unit_id][_idx_bucket_idx]) {
      // element is in bucket currently referenced by this iterator:
      _idx_bucket_phase += offset;
      _idx_local_idx    += offset;
    } else {
      // iterate units:
      for (; _idx_unit_id < _bucket_cumul_sizes->size(); ++_idx_unit_id) {
        auto unit_bkt_sizes = (*_bucket_cumul_sizes)[_idx_unit_id];
        DASH_LOG_TRACE("GlobBucketIter.increment",
                       "unit:",                _idx_unit_id,
                       "cumul. bucket sizes:", unit_bkt_sizes);
        // iterate the unit's bucket sizes:
        for (; _idx_bucket_idx < unit_bkt_sizes.size(); ++_idx_bucket_idx) {
          auto bucket_size = unit_bkt_sizes[_idx_bucket_idx];
          if (offset >= bucket_size) {
            _idx_local_idx += bucket_size;
            offset         -= bucket_size;
          } else {
            _idx_local_idx    += offset;
            _idx_bucket_phase  = offset;
            break;
          }
          // advance to next bucket:
          _idx_bucket_phase = 0;
        }
        // advance to next unit:
        _idx_local_idx  = 0;
        _idx_bucket_idx = 0;
      }
    }
    DASH_LOG_TRACE("GlobBucketIter.increment >",
                   "unit:",   _idx_unit_id,
                   "lidx:",   _idx_local_idx,
                   "bidx:",   _idx_bucket_idx,
                   "bphase:", _idx_bucket_phase);
  }

  void decrement(int offset)
  {
    DASH_LOG_TRACE_VAR("GlobBucketIter.decrement", offset);
    if (offset > _idx) {
      DASH_THROW(dash::exception::OutOfRange,
                 "offset " << offset << " is out of range");
    }
    _idx -= offset;
    if (offset <= _idx_bucket_phase) {
      // element is in bucket currently referenced by this iterator:
      _idx_bucket_phase -= offset;
      _idx_local_idx    -= offset;
    } else {
      offset -= _idx_bucket_phase;
      // iterate units:
      for (; _idx_unit_id >= 0; --_idx_unit_id) {
        auto unit_bkt_sizes = (*_bucket_cumul_sizes)[_idx_unit_id];
        DASH_LOG_TRACE("GlobBucketIter.decrement",
                       "unit:",                _idx_unit_id,
                       "cumul. bucket sizes:", unit_bkt_sizes);
        // iterate the unit's bucket sizes:
        for (; _idx_bucket_idx >= 0; --_idx_bucket_idx) {
          auto bucket_size = unit_bkt_sizes[_idx_bucket_idx];
          if (offset >= bucket_size) {
            _idx_local_idx -= bucket_size;
            offset         -= bucket_size;
          } else {
            _idx_local_idx    -= offset;
            _idx_bucket_phase  = offset;
            break;
          }
          // advance to previous bucket:
          _idx_bucket_phase = bucket_size - 1;
        }
        // advance to previous unit:
        _idx_local_idx  = 0;
        _idx_bucket_idx = unit_bkt_sizes.size() - 1;
      }
    }
    DASH_LOG_TRACE("GlobBucketIter.decrement >",
                   "unit:",   _idx_unit_id,
                   "lidx:",   _idx_local_idx,
                   "bidx:",   _idx_bucket_idx,
                   "bphase:", _idx_bucket_phase);
  }

private:
  /// Global memory used to dereference iterated values.
  GlobMemType            * _globmem            = nullptr;
  /// Mapping unit id to buckets in the unit's attached local storage.
  bucket_cumul_sizes_map * _bucket_cumul_sizes = nullptr;
  /// Pointer to first element in local data space.
  local_pointer            _lbegin;
  /// Current position of the iterator in global canonical index space.
  index_type               _idx                = 0;
  /// Maximum position allowed for this iterator.
  index_type               _max_idx            = 0;
  /// Unit id of the active unit.
  dart_unit_t              _myid;
  /// Unit id at the iterator's current position.
  dart_unit_t              _idx_unit_id        = DART_UNDEFINED_UNIT_ID;
  /// Logical offset in local index space at the iterator's current position.
  index_type               _idx_local_idx      = -1;
  /// Local bucket index at the iterator's current position.
  index_type               _idx_bucket_idx     = -1;
  /// Element offset in bucket at the iterator's current position.
  index_type               _idx_bucket_phase   = -1;

}; // class GlobBucketIter

/**
 * Resolve the number of elements between two global bucket iterators.
 *
 * \complexity  O(1)
 *
 * \ingroup     Algorithms
 */
template<
  typename ElementType,
  class    GlobMemType,
  class    Pointer,
  class    Reference>
auto distance(
  /// Global iterator to the first position in the global sequence
  const dash::internal::GlobBucketIter<
          ElementType, GlobMemType, Pointer, Reference> & first,
  /// Global iterator to the final position in the global sequence
  const dash::internal::GlobBucketIter<
          ElementType, GlobMemType, Pointer, Reference> & last)
-> typename GlobMemType::index_type
{
  return last - first;
}

#if 0
template<
  typename ElementType,
  class    GlobMemType,
  class    Pointer,
  class    Reference>
std::ostream & operator<<(
  std::ostream & os,
  const dash::internal::GlobBucketIter<
          ElementType, GlobMemType, Pointer, Reference> & it)
{
  std::ostringstream ss;
  auto ptr = it.dart_gptr();
  ss << "dash::internal::GlobBucketIter<"
     << typeid(ElementType).name() << ">("
     << "idx:"  << it.pos() << ", "
     << "gptr:" << ptr      << ")";
  return operator<<(os, ss.str());
}
#endif

} // namespace internal
} // namespace dash

#endif // DASH__INTERNAL__ALLOCATOR__GLOB_BUCKET_ITER_H__INCLUDED