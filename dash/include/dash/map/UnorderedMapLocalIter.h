#ifndef DASH__MAP__UNORDERED_MAP_LOCAL_ITER_H__INCLUDED
#define DASH__MAP__UNORDERED_MAP_LOCAL_ITER_H__INCLUDED

#include <dash/dart/if/dart.h>

#include <dash/Types.h>
#include <dash/GlobPtr.h>
#include <dash/Allocator.h>
#include <dash/Team.h>
#include <dash/Onesided.h>

#include <dash/map/UnorderedMapLocalIter.h>

#include <dash/internal/Logging.h>

#include <type_traits>
#include <list>
#include <vector>
#include <iterator>
#include <sstream>
#include <iostream>

namespace dash {

// Forward-declaration
template<
  typename Key,
  typename Mapped,
  typename HashUnit,
  typename Hash,
  typename Pred,
  typename Alloc,
  typename RehashPolicy
>
class UnorderedMap;

template<
  typename Key,
  typename Mapped,
  typename HashUnit,
  typename Hash,
  typename Pred,
  typename Alloc,
  typename RehashPolicy
>
class UnorderedMapLocalIter
: public std::iterator<
           std::random_access_iterator_tag,
           std::pair<const Key, Mapped>,
           dash::default_index_t,
           std::pair<const Key, Mapped> *,
           std::pair<const Key, Mapped> &>
{
  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  friend std::ostream & dash::operator<<(
    std::ostream & os,
    const dash::UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & it);

private:
  typedef UnorderedMapLocalIter<Key, Mapped, HashUnit, Hash, Pred, Alloc, RehashPolicy>
    self_t;

  typedef UnorderedMap<Key, Mapped, HashUnit, Hash, Pred, Alloc, RehashPolicy>
    map_t;

public:
  typedef typename map_t::value_type                              value_type;
#if 0
  typedef typename map_t::index_type                              index_type;
  typedef typename map_t::size_type                                size_type;
#else
  typedef dash::default_index_t                                   index_type;
  typedef dash::default_size_t                                     size_type;
#endif

  typedef       value_type *                                         pointer;
  typedef const value_type *                                   const_pointer;
  typedef       value_type &                                       reference;
  typedef const value_type &                                 const_reference;

  typedef struct {
    team_unit_t unit;
    index_type  index;
  } local_index;

public:
  /**
   * Default constructor.
   */
  UnorderedMapLocalIter()
  : UnorderedMapLocalIter(nullptr)
  { }

  /**
   * Constructor, creates iterator at specified global position.
   */
  UnorderedMapLocalIter(
    map_t       * map,
    index_type    local_position)
  : _map(map),
    _idx(local_position),
    _myid(dash::Team::GlobalUnitID())
  {
    reset_idx_bucket_phase();
    DASH_LOG_TRACE("UnorderedMapLocalIter(map,lpos)()");
    DASH_LOG_TRACE_VAR("UnorderedMapLocalIter(map,lpos)", _idx);
    DASH_LOG_TRACE("UnorderedMapLocalIter(map,lpos) >");
  }

  /**
   * Copy constructor.
   */
  UnorderedMapLocalIter(
    const self_t & other) = default;

  /**
   * Assignment operator.
   */
  self_t & operator=(
    const self_t & other) = default;

  /**
   * Null-pointer constructor.
   */
  UnorderedMapLocalIter(std::nullptr_t)
  : _map(nullptr),
    _idx(-1),
    _myid(DART_UNDEFINED_UNIT_ID),
    _is_nullptr(true)
  {
    DASH_LOG_TRACE("UnorderedMapLocalIter(nullptr)");
  }

  /**
   * Null-pointer assignment operator.
   */
  self_t & operator=(std::nullptr_t) noexcept
  {
    _is_nullptr = true;
    return *this;
  }

  inline bool operator==(std::nullptr_t) const noexcept
  {
    return _is_nullptr;
  }

  inline bool operator!=(std::nullptr_t) const noexcept
  {
    return !_is_nullptr;
  }

  /**
   * Random access operator.
   */
  reference operator[](index_type offset)
  {
    auto res = *this;
    res += offset;
    return *this;
  }

  /**
   * Type conversion operator to native pointer.
   *
   * \return  A global reference to the element at the iterator's position
   */
  explicit operator pointer() const
  {
    typedef typename map_t::local_node_iterator local_iter_t;
    if (_is_nullptr) {
      return nullptr;
    }
    // TODO: Must be extended for correctness: _idx refers to local iteration
    //       space, not local memory space. Undefined behaviour if local
    //       memory space has gaps, e.g. after erasing elements.
    local_iter_t l_it = _map->globmem().lbegin();
    return pointer(l_it + static_cast<index_type>(gmem_idx_at_lidx()));
  }

  /**
   * Dereference operator.
   *
   * \return  A global reference to the element at the iterator's position.
   */
  reference operator*() const
  {
    typedef typename map_t::local_node_iterator local_iter_t;
    DASH_ASSERT(!_is_nullptr);
    // TODO: Must be extended for correctness: _idx refers to local iteration
    //       space, not local memory space. Undefined behaviour if local
    //       memory space has gaps, e.g. after erasing elements.
    local_iter_t l_it = _map->globmem().lbegin();
    return *pointer(l_it + static_cast<index_type>(gmem_idx_at_lidx()));
  }

  /**
   * Explicit conversion to \c dart_gptr_t.
   *
   * \return  A DART global pointer to the element at the iterator's
   *          position
   */
  dart_gptr_t dart_gptr() const
  {
    DASH_LOG_TRACE_VAR("UnorderedMapLocalIter.dart_gptr()", _idx);
    dart_gptr_t dart_gptr = DART_GPTR_NULL;
    if (!_is_nullptr) {
      dart_gptr = _map->globmem().at(_myid, gmem_idx_at_lidx()).dart_gptr();
    }
    DASH_LOG_TRACE_VAR("UnorderedMapLocalIter.dart_gptr >", dart_gptr);
    return dart_gptr;
  }

  /**
   * Checks whether the element referenced by this global iterator is in
   * the calling unit's local memory.
   */
  constexpr bool is_local() const noexcept
  {
    return true;
  }

  /**
   * Unit and local offset at the iterator's position.
   */
  inline local_index lpos() const noexcept
  {
    local_index local_pos;
    local_pos.unit  = _myid;
    local_pos.index = _idx;
    return local_pos;
  }

  /**
   * Position of the iterator in global index space.
   */
  inline index_type pos() const noexcept
  {
    return _idx;
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

  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  inline bool operator==(
    const UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & other) const
  {
    return (this == std::addressof(other) || _idx == other._idx);
  }

  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  inline bool operator!=(
    const UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & other) const
  {
    return !(*this == other);
  }

  self_t & operator+=(index_type offset)
  {
    increment(offset);
    return *this;
  }

  self_t & operator-=(index_type offset)
  {
    decrement(offset);
    return *this;
  }

  self_t operator+(index_type offset) const
  {
    auto res = *this;
    res += offset;
    return res;
  }

  self_t operator-(index_type offset) const
  {
    auto res = *this;
    res -= offset;
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

  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  inline bool operator<(
    const UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & other) const
  {
    return (_idx < other._idx);
  }

  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  inline bool operator<=(
    const UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & other) const
  {
    return (_idx <= other._idx);
  }

  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  inline bool operator>(
    const UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & other) const
  {
    return (_idx > other._idx);
  }

  template<typename K_, typename M_, typename HU_, typename H_, typename P_, typename A_, typename R_>
  inline bool operator>=(
    const UnorderedMapLocalIter<K_, M_, HU_, H_, P_, A_, R_> & other) const
  {
    return (_idx >= other._idx);
  }

private:
  /**
   * Advance pointer by specified position offset.
   */
  void increment(index_type offset)
  {
    DASH_LOG_TRACE("UnorderedMapLocalIter.increment()",
                   "unit:",   _myid,
                   "lidx:",   _idx,
                   "offset:", offset);
    if (offset == 0) return;

    if (offset < 0) {
      decrement(-offset);
      return;
    }


    _idx += offset;

    auto const & u_bucket_cumul_sizes = _map->_local_bucket_cumul_sizes[_myid];

    auto const lsz = u_bucket_cumul_sizes.size() ?
      u_bucket_cumul_sizes.back() : 0;

    if (_idx > lsz) {
      //if _idx equals lsz then it is only the special local end iterator
      DASH_LOG_ERROR("UnorderedMapLocalIter.increment", "index out of range", _idx);
      reset_idx_bucket_phase();
      return;
    }

    if (_idx_bucket > -1) {
        //Update bucket phase and bucket idx

        for (;
             _idx_bucket < u_bucket_cumul_sizes.size() && u_bucket_cumul_sizes[_idx_bucket] < _idx;
             ++_idx_bucket)
          ;

        _idx_bucket_phase =
            (_idx_bucket > 0)
                ? _idx - u_bucket_cumul_sizes[_idx_bucket - 1]
                : _idx;
    }
    else {
      reset_idx_bucket_phase();
    }

    DASH_LOG_TRACE("UnorderedMapLocalIter.increment >");
  }

  /**
   * Decrement pointer by specified position offset.
   */
  void decrement(index_type offset)
  {
    DASH_LOG_TRACE("UnorderedMapLocalIter.decrement()",
                   "unit:",   _myid,
                   "lidx:",   _idx,
                   "offset:", -offset);
    _idx -= offset;
    DASH_LOG_TRACE("UnorderedMapLocalIter.decrement >");
  }

  void reset_idx_bucket_phase() {
    auto& u_bucket_cumul_sizes = _map->_local_bucket_cumul_sizes[_myid];


    if (u_bucket_cumul_sizes.size() == 0 || u_bucket_cumul_sizes.back() == 0) {
      //Update to local begin
      _idx = 0;
      _idx_bucket = 0;
      _idx_bucket_phase = 0;
      return;
    } else if (_idx >= u_bucket_cumul_sizes.back()) {
      //Update to
      _idx = u_bucket_cumul_sizes.back();

      //Find last filled bucket
      auto const last_bkt_size = u_bucket_cumul_sizes.back();

      index_type bkt =
          (u_bucket_cumul_sizes.size() >
          1) ? u_bucket_cumul_sizes.size() - 2 : 0;

      for (; bkt >= 0 && u_bucket_cumul_sizes[bkt] == last_bkt_size; --bkt);

      //Last bucket
      _idx_bucket = bkt;

      //Set bucket phase to mark local end
      _idx_bucket_phase =
          (_idx_bucket > 0)
              ? last_bkt_size - u_bucket_cumul_sizes[_idx_bucket - 1]
              : last_bkt_size;
      return;
    }
    // Find corresponding bucket and bucket phase

    for (_idx_bucket = 0;
         _idx_bucket < u_bucket_cumul_sizes.size() &&
         u_bucket_cumul_sizes[_idx_bucket] < _idx;
         ++_idx_bucket) {
    }

    if (_idx_bucket > 0) {
      _idx_bucket_phase = _idx - u_bucket_cumul_sizes[_idx_bucket - 1];
    } else {
      _idx_bucket_phase = _idx;
    }

    DASH_LOG_TRACE("UnorderedMapLocalIter.reset_idx_bucket_phase",
                   "(local index : bucket idx : bucket_phase) -->",
                   _idx, ":", _idx_bucket, ":",
                   _idx_bucket_phase);
  }

  inline index_type gmem_idx_at_lidx() const noexcept{
    DASH_ASSERT_GT(_idx_bucket, -1, "UnorderedMapLocalIter: invalid state");

    index_type gmem_idx;
    if (_idx_bucket > 0) {
      gmem_idx =
          (_idx_bucket - 1) * _map->_local_buffer_size + _idx_bucket_phase;
    }
    else {
      DASH_ASSERT_EQ(_idx, _idx_bucket_phase, "UnorderedMapLocalIter: invalid state");
      gmem_idx = _idx;
    }

    DASH_LOG_TRACE("UnorderedMapLocalIter.gmem_idx_at_lidx", _idx,
                   "-->", gmem_idx);

    return gmem_idx;
  }

private:
  /// Pointer to referenced map instance.
  map_t                  * _map           = nullptr;
  /// Current position of the iterator in local canonical index space.
  index_type               _idx                 = -1;
  /// Current bucket idx of the iterator in local cannonical index space
  index_type               _idx_bucket          = -1;
  /// Current bucket phase of the iterator in local cannonical index space
  index_type               _idx_bucket_phase    = -1;
  /// Unit id of the active unit.
  team_unit_t              _myid          = DART_UNDEFINED_TEAM_UNIT_ID;
  /// Whether the iterator represents a null pointer.
  bool                     _is_nullptr    = false;

}; // class UnorderedMapLocalIter

template<
  typename Key,
  typename Mapped,
  typename HashUnit,
  typename Hash,
  typename Pred,
  typename Alloc,
  typename RehashPolicy>
std::ostream & operator<<(
  std::ostream & os,
  const dash::UnorderedMapLocalIter<
          Key, Mapped, HashUnit, Hash, Pred, Alloc, RehashPolicy> & it)
{
  std::ostringstream ss;
  ss << "dash::UnorderedMapLocalIter<"
     << typeid(Key).name()    << ","
     << typeid(Mapped).name() << ">"
     << "("
     << "unit:" << it._myid   << ", "
     << "lidx:" << it._idx
     << ")";
  return operator<<(os, ss.str());
}

} // namespace dash

#endif // DASH__MAP__UNORDERED_MAP_LOCAL_ITER_H__INCLUDED
