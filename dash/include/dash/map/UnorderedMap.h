#ifndef DASH__MAP__UNORDERED_MAP_H__INCLUDED
#define DASH__MAP__UNORDERED_MAP_H__INCLUDED

#include <dash/Types.h>
#include <dash/GlobRef.h>
#include <dash/Team.h>
#include <dash/Exception.h>
#include <dash/Array.h>
#include <dash/Allocator.h>
#include <dash/Meta.h>

#include <dash/memory/GlobHeapMem.h>

#include <dash/atomic/GlobAtomicRef.h>

#include <dash/map/UnorderedMapLocalRef.h>
#include <dash/map/UnorderedMapLocalIter.h>
#include <dash/map/UnorderedMapGlobIter.h>
#include <dash/map/HashPolicy.h>

#include <dash/algorithm/Find.h>
#include <dash/algorithm/Fill.h>

#include <iterator>
#include <utility>
#include <limits>
#include <vector>
#include <functional>
#include <algorithm>
#include <cstddef>
#include <cstring>


namespace dash {


#ifndef DOXYGEN

template <typename Key,
          typename Mapped,
          typename HashUnit = dash::HashUnitLocal<Key>,
          typename Hash = dash::Hash<Key>,
          typename Pred = std::equal_to<Key>,
          typename Alloc = dash::allocator::EpochSynchronizedAllocator<
              std::pair<const Key, Mapped>>,
          typename RehashPolicy = PrimeRehashPolicy>
class UnorderedMap {
  static_assert(
    dash::is_container_compatible<Key>::value &&
    dash::is_container_compatible<Mapped>::value,
    "Type not supported for DASH containers");

  template<typename K_, typename M_, typename HU_, typename H_, typename P_,
    typename A_, typename RH_>
  friend class UnorderedMapLocalRef;

  template<typename K_, typename M_, typename HU_, typename H_, typename P_,
    typename A_, typename RH_>
  friend class UnorderedMapGlobIter;

  template<typename K_, typename M_, typename HU_, typename H_, typename P_,
    typename A_, typename RH_>
  friend class UnorderedMapLocalIter;

private:
  typedef UnorderedMap<Key, Mapped, HashUnit, Hash, Pred, Alloc, RehashPolicy>
    self_t;

  typedef dash::util::Timer<dash::util::TimeMeasure::Clock>
    Timer;

  using LocalAlloc =
      dash::allocator::LocalSpaceAllocator<std::pair<const Key, Mapped>>;
  using PoolAlloc = dash::SimpleMemoryPool<double, LocalAlloc>;
  using EpochSyncAlloc = dash::allocator::EpochSynchronizedAllocator<std::pair<const Key, Mapped>>;
  using AllocatorTraits = dash::allocator_traits<EpochSyncAlloc>;

public:
  static constexpr size_t DEFAULT_BUFFER_SIZE =
    4096 / sizeof(std::pair<const Key, Mapped>);

public:
  typedef Key                                                                  key_type;
  typedef Mapped                                                            mapped_type;
  typedef HashUnit                                                        global_hasher;
  typedef Hash                                                             local_hasher;
  typedef RehashPolicy                                               rehash_policy_type;
  typedef Pred                                                                key_equal;
  typedef std::pair<const key_type, mapped_type>                             value_type;

  typedef EpochSyncAlloc                                                  allocator_type;

  typedef dash::default_index_t                                              index_type;
  typedef dash::default_index_t                                         difference_type;
  typedef dash::default_size_t                                                size_type;

  typedef UnorderedMapLocalRef<Key, Mapped, HashUnit, Hash, Pred, Alloc,
                               RehashPolicy>
      local_type;

  typedef dash::GlobHeapMem<value_type, allocator_type>        glob_mem_type;

  typedef typename glob_mem_type::reference                        reference;
  typedef typename glob_mem_type::const_reference            const_reference;

  typedef typename reference::template rebind<mapped_type>::other
    mapped_type_reference;
  typedef typename const_reference::template rebind<mapped_type>::other
    const_mapped_type_reference;

  typedef typename glob_mem_type::pointer
    node_iterator;
  typedef typename glob_mem_type::const_pointer
    const_node_iterator;
  typedef typename glob_mem_type::local_pointer
    local_node_iterator;
  typedef typename glob_mem_type::const_local_pointer
    const_local_node_iterator;

  typedef typename glob_mem_type::pointer
    local_node_pointer;
  typedef typename glob_mem_type::const_pointer
    const_local_node_pointer;

  typedef UnorderedMapGlobIter<Key, Mapped, HashUnit, Hash, Pred, Alloc,
          RehashPolicy>
    iterator;
  typedef UnorderedMapGlobIter<Key, Mapped, HashUnit, Hash, Pred, Alloc,
          RehashPolicy>
    const_iterator;
  typedef typename std::reverse_iterator<iterator>
    reverse_iterator;
  typedef typename std::reverse_iterator<const_iterator>
    const_reverse_iterator;

  typedef UnorderedMapLocalIter<Key, Mapped, HashUnit, Hash, Pred, Alloc,
                                RehashPolicy>
      local_pointer;
  typedef UnorderedMapLocalIter<Key, Mapped, HashUnit, Hash, Pred, Alloc,
                                RehashPolicy>
      const_local_pointer;
  typedef UnorderedMapLocalIter<Key, Mapped, HashUnit, Hash, Pred, Alloc,
                                RehashPolicy>
      local_iterator;
  typedef UnorderedMapLocalIter<Key, Mapped, HashUnit, Hash, Pred, Alloc,
                                RehashPolicy>
      const_local_iterator;
  typedef typename std::reverse_iterator<local_iterator>
    reverse_local_iterator;
  typedef typename std::reverse_iterator<const_local_iterator>
    const_reverse_local_iterator;

  typedef typename glob_mem_type::local_reference
    local_reference;
  typedef typename glob_mem_type::const_local_reference
    const_local_reference;

  typedef dash::Array<
            size_type, int, dash::CSRPattern<1, dash::ROW_MAJOR, int> >
    local_sizes_map;

private:
  /// Team containing all units interacting with the map.
  dash::Team           * _team            = nullptr;
  /// DART id of the local unit.
  team_unit_t            _myid{DART_UNDEFINED_UNIT_ID};
  /// Global memory allocation and -access.
  glob_mem_type        * _globmem         = nullptr;
  /// Iterator to initial element in the map.
  iterator               _begin           = nullptr;
  /// Iterator past the last element in the map.
  iterator               _end             = nullptr;
  /// Number of elements in the map.
  size_type              _remote_size     = 0;
  /// Native pointer to first local element in the map.
  local_iterator         _lbegin          = nullptr;
  /// Native pointer past the last local element in the map.
  local_iterator         _lend            = nullptr;
  /// Mapping units to their number of local map elements.
  local_sizes_map        _local_sizes;
  /// Mapping units to their local bucket sizes
  local_sizes_map        _local_bucket_sizes;
  /// Number of buckets in each unit
  size_type              _n_local_buckets;
  /// Cumulative (prefix sum) local sizes of all units.
  std::vector<size_type> _local_cumul_sizes;
  /// Cumulative (prefix sum) local sizes of all buckets at all units
  std::vector<std::vector<size_type>> _local_bucket_cumul_sizes;

  std::vector<typename glob_mem_type::pointer> _buckets;
  /// Iterators to elements in local memory space that are marked for move
  /// to remote unit in next commit.
  std::vector<iterator>  _move_elements;
  /// Global pointer to local element in _local_sizes.
  dart_gptr_t            _local_size_gptr = DART_GPTR_NULL;
  /// Hash type for mapping of key to unit
  global_hasher                 _key_global_hash;
  /// Hash type for mapping of key to local offset
  local_hasher                  _key_local_hash;
  /// Hash type for mapping of key to local offset
  rehash_policy_type             _rehash_policy;
  /// Predicate for key comparison.
  key_equal              _key_equal;
  /// Capacity of local buffer containing locally added node elements that
  /// have not been committed to global memory yet.
  /// Default is 4 KB.
  size_type              _local_buffer_size
                           = DEFAULT_BUFFER_SIZE;

public:
  /// Local proxy object, allows use in range-based for loops.
  local_type local;

public:
  UnorderedMap(
    size_type   nelem = 0,
    Team      & team  = dash::Team::All())
  : _team(&team),
    _myid(team.myid()),
    _key_global_hash(team),
    local(this)
  {
    DASH_LOG_TRACE_VAR("UnorderedMap(nelem,team)", nelem);
    if (_team->size() > 0) {
      allocate(nelem, team);
    }
    DASH_LOG_TRACE("UnorderedMap(nelem,team) >");
  }

  UnorderedMap(
    size_type   nelem,
    size_type   nlbuf,
    Team      & team  = dash::Team::All())
  : _team(&team),
    _myid(team.myid()),
    _key_global_hash(team),
    _local_buffer_size(nlbuf),
    local(this)
  {
    DASH_LOG_TRACE("UnorderedMap(nelem,nlbuf,team)",
                   "nelem:", nelem, "nlbuf:", nlbuf);
    if (_team->size() > 0) {
      allocate(nelem, team);
    }
    DASH_LOG_TRACE("UnorderedMap(nelem,nlbuf,team) >");
  }

  ~UnorderedMap()
  {
    DASH_LOG_TRACE_VAR("UnorderedMap.~UnorderedMap()", this);
    deallocate();
    DASH_LOG_TRACE_VAR("UnorderedMap.~UnorderedMap >", this);
  }

  //////////////////////////////////////////////////////////////////////////
  // Distributed container
  //////////////////////////////////////////////////////////////////////////

  inline const Team & team() const noexcept
  {
    if (_team != nullptr) {
      return *_team;
    }
    return dash::Team::Null();
  }

  inline const glob_mem_type & globmem() const
  {
    return *_globmem;
  }

  //////////////////////////////////////////////////////////////////////////
  // Dynamic distributed memory
  //////////////////////////////////////////////////////////////////////////

  void barrier()
  {
    DASH_LOG_TRACE_VAR("UnorderedMap.barrier()", _team->dart_id());
    // Apply changes in local memory spaces to global memory space:
    if (_globmem != nullptr) {
      _globmem->commit();
    }
    // Accumulate local sizes of remote units:
    _local_sizes.barrier();
    _remote_size = 0;
    for (int u = 0; u < _team->size(); ++u) {
      size_type local_size_u;
      if (u != _myid) {
        local_size_u = _local_sizes[u];
        _remote_size += local_size_u;
        //TODO copy all buckets from remote to local and accumulate
      } else {
        local_size_u = _local_sizes.local[0];
      }
      _local_cumul_sizes[u] = local_size_u;
      if (u > 0) {
        _local_cumul_sizes[u] += _local_cumul_sizes[u-1];
      }
      DASH_LOG_TRACE("UnorderedMap.barrier",
                     "local size at unit", u, ":", local_size_u,
                     "cumulative size:", _local_cumul_sizes[u]);
    }
    auto new_size = size();
    DASH_LOG_TRACE("UnorderedMap.barrier", "new size:", new_size);
    DASH_ASSERT_EQ(_remote_size, new_size - _local_sizes.local[0],
                   "invalid size after global commit");
    _begin = iterator(this, 0);
    _end   = iterator(this, new_size);
    DASH_LOG_TRACE("UnorderedMap.barrier >", "passed barrier");
  }

  bool allocate(
    /// Initial global capacity of the container.
    size_type    nelem = 0,
    /// Team containing all units associated with the container.
    dash::Team & team  = dash::Team::All())
  {
    DASH_LOG_TRACE("UnorderedMap.allocate()");
    DASH_LOG_TRACE_VAR("UnorderedMap.allocate", nelem);
    DASH_LOG_TRACE_VAR("UnorderedMap.allocate", _local_buffer_size);
    if (_team == nullptr || *_team == dash::Team::Null()) {
      DASH_LOG_TRACE("UnorderedMap.allocate",
                     "initializing with specified team -",
                     "team size:", team.size());
      _team = &team;
      DASH_LOG_TRACE_VAR("UnorderedMap.allocate", team.dart_id());
    } else {
      DASH_LOG_TRACE("UnorderedMap.allocate",
                     "initializing with initial team");
    }
    DASH_ASSERT_GT(_local_buffer_size, 0, "local buffer size must not be 0");
    if (nelem < _team->size() * _local_buffer_size) {
      nelem = _team->size() * _local_buffer_size;
      DASH_LOG_TRACE("UnorderedMap.allocate", "nelem increased to", nelem);
    }
    _key_global_hash    = global_hasher(*_team);
    _remote_size = 0;
    //initial capacity for n elements
    auto _n_local_buckets = dash::math::div_ceil(nelem, _team->size());
    //required number of buckets according to rehash policy
    auto prime_index = _rehash_policy.next_size_over(_n_local_buckets);

    //Fill all initial bucket sizes with 0
    _local_bucket_sizes.allocate(_team->size() * _n_local_buckets, dash::BLOCKED, *_team);
    dash::fill(_local_bucket_sizes.begin(), _local_bucket_sizes.end(), 0);

    auto lcap = _n_local_buckets * _local_buffer_size;
    // Initialize members:
    _myid        = _team->myid();

    DASH_LOG_TRACE("UnorderedMap.allocate", "initialize global memory,",
                   "local capacity:", lcap);

    _globmem     = new glob_mem_type(lcap, *_team);
    DASH_LOG_TRACE("UnorderedMap.allocate", "global memory initialized");

    _local_cumul_sizes.resize(_team->size(), 0);
    _local_bucket_cumul_sizes.resize(_team->size());
    // Initialize local sizes with 0:
    _local_sizes.allocate(_team->size(), dash::BLOCKED, *_team);
    _local_sizes.local[0] = 0;

    _local_size_gptr      = _local_sizes[_myid].dart_gptr();

    // Global iterators:
    _begin       = iterator(this, 0);
    _end         = _begin;
    DASH_LOG_TRACE_VAR("UnorderedMap.allocate", _begin);
    DASH_LOG_TRACE_VAR("UnorderedMap.allocate", _end);
    // Local iterators:
    _lbegin      = local_iterator(this, 0);
    _lend        = _lbegin;
    DASH_LOG_TRACE_VAR("UnorderedMap.allocate", _lbegin);
    DASH_LOG_TRACE_VAR("UnorderedMap.allocate", _lend);
    // Register deallocator of this map instance at the team
    // instance that has been used to initialized it:
    _team->register_deallocator(
             this, std::bind(&UnorderedMap::deallocate, this));

    _rehash_policy.commit(prime_index);
    // Assure all units are synchronized after allocation, otherwise
    // other units might start working on the map before allocation
    // completed at all units:
    if (dash::is_initialized()) {
      DASH_LOG_TRACE("UnorderedMap.allocate",
                     "waiting for allocation of all units");
      _team->barrier();
    }
    DASH_LOG_TRACE("UnorderedMap.allocate >", "finished");
    return true;
  }

  void deallocate()
  {
    DASH_LOG_TRACE_VAR("UnorderedMap.deallocate()", this);
    // Assure all units are synchronized before deallocation, otherwise
    // other units might still be working on the map:
    if (dash::is_initialized()) {
      barrier();
    }
    // Remove this function from team deallocator map to avoid
    // double-free:
    _team->unregister_deallocator(
      this, std::bind(&UnorderedMap::deallocate, this));
    // Deallocate map elements:
    DASH_LOG_TRACE_VAR("UnorderedMap.deallocate()", _globmem);
    if (_globmem != nullptr) {
      delete _globmem;
      _globmem = nullptr;
    }
    _local_cumul_sizes    = std::vector<size_type>(_team->size(), 0);
    _local_sizes.local[0] = 0;
    _remote_size          = 0;
    _begin                = iterator();
    _end                  = _begin;
    DASH_LOG_TRACE_VAR("UnorderedMap.deallocate >", this);
  }

  //////////////////////////////////////////////////////////////////////////
  // Global Iterators
  //////////////////////////////////////////////////////////////////////////

  inline iterator & begin() noexcept
  {
    return _begin;
  }

  inline const_iterator & begin() const noexcept
  {
    return _begin;
  }

  inline const_iterator & cbegin() const noexcept
  {
    return const_cast<const self_t *>(this)->begin();
  }

  inline iterator & end() noexcept
  {
    return _end;
  }

  inline const_iterator & end() const noexcept
  {
    return _end;
  }

  inline const_iterator & cend() const noexcept
  {
    return const_cast<const self_t *>(this)->end();
  }

  //////////////////////////////////////////////////////////////////////////
  // Local Iterators
  //////////////////////////////////////////////////////////////////////////

  inline local_iterator & lbegin() noexcept
  {
    return _lbegin;
  }

  inline const_local_iterator & lbegin() const noexcept
  {
    return _lbegin;
  }

  inline const_local_iterator & clbegin() const noexcept
  {
    return const_cast<const self_t *>(this)->lbegin();
  }

  inline local_iterator & lend() noexcept
  {
    return _lend;
  }

  inline const_local_iterator & lend() const noexcept
  {
    return _lend;
  }

  inline const_local_iterator & clend() const noexcept
  {
    return const_cast<self_t *>(this)->lend();
  }

  //////////////////////////////////////////////////////////////////////////
  // Capacity
  //////////////////////////////////////////////////////////////////////////

  constexpr size_type max_size() const noexcept
  {
    return std::numeric_limits<key_type>::max();
  }

  inline size_type size() const noexcept
  {
    return _remote_size + _local_sizes.local[0];
  }

  inline size_type capacity() const noexcept
  {
    return _globmem->size();
  }

  inline bool empty() const noexcept
  {
    return size() == 0;
  }

  inline size_type lsize() const noexcept
  {
    return _local_sizes.local[0];
  }

  inline size_type lcapacity() const noexcept
  {
    return _globmem != nullptr
           ? _globmem->local_size()
           : 0;
  }

  //////////////////////////////////////////////////////////////////////////
  // Element Access
  //////////////////////////////////////////////////////////////////////////

  mapped_type_reference operator[](const key_type & key)
  {
    DASH_LOG_TRACE("UnorderedMap.[]()", "key:", key);
    iterator      git_value   = insert(
                                   std::make_pair(key, mapped_type()))
                                .first;
    DASH_LOG_TRACE_VAR("UnorderedMap.[]", git_value);
    dart_gptr_t   gptr_mapped = git_value.dart_gptr();
    value_type  * lptr_value  = static_cast<value_type *>(
                                  git_value.local());
    mapped_type * lptr_mapped = nullptr;

    _lptr_value_to_mapped(lptr_value, gptr_mapped, lptr_mapped);
    // Create global reference to mapped value member in element:
    mapped_type_reference mapped(gptr_mapped,
                                 lptr_mapped);
    DASH_LOG_TRACE("UnorderedMap.[] >", mapped);
    return mapped;
  }

  const_mapped_type_reference at(const key_type &key) const
  {
    DASH_LOG_TRACE("UnorderedMap.at() const", "key:", key);
    auto found = find(key);
    if (found == _end) {
      // No equivalent key in map, throw:
      DASH_THROW(dash::exception::InvalidArgument, "No element in map for key "
                                                       << key);
    }

    dart_gptr_t gptr_mapped = iterator(this, found.pos()).dart_gptr();

    value_type *lptr_value = static_cast<value_type *>(found.local());
    mapped_type *lptr_mapped = nullptr;

    _lptr_value_to_mapped(lptr_value, gptr_mapped, lptr_mapped);
    // Create global reference to mapped value member in element:
    const_mapped_type_reference mapped(gptr_mapped, lptr_mapped);
    return mapped;
  }

  mapped_type_reference at(const key_type & key)
  {
    DASH_LOG_TRACE("UnorderedMap.at()", "key:", key);
    // TODO: Unoptimized, currently calls find(key) twice as operator[](key)
    //       calls insert(key).
    const_iterator git_value = find(key);
    if (git_value == _end) {
      // No equivalent key in map, throw:
      DASH_THROW(
        dash::exception::InvalidArgument,
        "No element in map for key " << key);
    }
    auto mapped = this->operator[](key);
    DASH_LOG_TRACE("UnorderedMap.at >", mapped);
    return mapped;
  }

  //////////////////////////////////////////////////////////////////////////
  // Element Lookup
  //////////////////////////////////////////////////////////////////////////

  size_type count(const key_type & key) const
  {
    DASH_LOG_TRACE_VAR("UnorderedMap.count()", key);
    size_type nelem = 0;
    if (find(key) != _end) {
      nelem = 1;
    }
    DASH_LOG_TRACE("UnorderedMap.count >", nelem);
    return nelem;
  }

  iterator find(const key_type & key)
  {
    DASH_LOG_TRACE_VAR("UnorderedMap.find()", key);
    auto const unit_bucket = bucket(key);

    auto const unit = unit_bucket.first;
    auto const u_bucket = unit_bucket.second;

    const_iterator found = do_find(unit, u_bucket, key);

    DASH_LOG_TRACE("UnorderedMap.find >", found);

    auto pos = found.lpos();
    return iterator{this, pos.unit, pos.index};
  }

  const_iterator find(const key_type & key) const
  {
    DASH_LOG_TRACE_VAR("UnorderedMap.find() const", key);
    auto const unit_bucket = bucket(key);

    auto const unit = unit_bucket.first;
    auto const u_bucket = unit_bucket.second;

    const_iterator found = do_find(unit, u_bucket, key);
    DASH_LOG_TRACE("UnorderedMap.find const >", found);

    return found;
  }

  //////////////////////////////////////////////////////////////////////////
  // Modifiers
  //////////////////////////////////////////////////////////////////////////

  std::pair<iterator, bool> insert(
    /// The element to insert.
    const value_type & value)
  {
    auto && key = value.first;
    DASH_LOG_TRACE("UnorderedMap.insert()", "key:", key);

    auto unit_bucket = bucket(key);

    auto result = std::make_pair(_end, false);

    DASH_ASSERT(_globmem != nullptr);
    // Look up existing element at given key:
    DASH_LOG_TRACE("UnorderedMap.insert", "element key lookup");
    const_iterator found = do_find(unit_bucket.first, unit_bucket.second, key);
    DASH_LOG_TRACE_VAR("UnorderedMap.insert", found);

    if (found != _end) {
      DASH_LOG_TRACE("UnorderedMap.insert", "key found");
      // Existing element found, no insertion:
      result.first  = iterator(this, found.pos());
    } else {
      DASH_LOG_TRACE("UnorderedMap.insert", "key not found");
      // Unit mapped to the new element's key by the hash function:
      DASH_LOG_TRACE("UnorderedMap.insert", "unit:", unit_bucket.first, "bucket: ", unit_bucket.second);
      // No element with specified key exists, insert new value.
      result = do_insert(unit_bucket.first, unit_bucket.second, value);
    }
    DASH_LOG_DEBUG("UnorderedMap.insert >",
                   (result.second ? "inserted" : "existing"), ":",
                   result.first);
    return result;
  }

  iterator insert(
    const_iterator hint,
    const value_type & value)
  {
    //Timer::timestamp_t ts_enter = Timer::Now(), ts_insert, ts_find, d_insert, d_find;
    auto key = value.first;
    auto mapped = value.second;

    DASH_ASSERT(_globmem != nullptr);
    DASH_LOG_DEBUG("UnorderedMap.insert()", "key:", key, "mapped:", mapped);

    auto unit_bucket = bucket(key);

    iterator found = find(key);

    /*
    if (_myid == unit_bucket.first) {
      DASH_LOG_TRACE("UnorderedMap.insert", "local element key lookup");
      //Begin of local bucket
      //
      auto bucket = unit_bucket.second;

      auto lidx = (bucket > 0) ? _local_bucket_cumul_sizes[_myid][bucket -1] : 0;

      local_iterator lstart{this, lidx};
      local_iterator lend{this, _local_bucket_cumul_sizes[_myid][bucket]};

      //ts_find = Timer::Now();
      const_local_iterator liter = std::find_if(
                  lstart, lend,
                   [&](const local_iterator & v) {
                     return _key_equal(v->first, key);
                   });
      //d_find = Timer::ElapsedSince(ts_find);

      if (liter != lend) {
        found = iterator(this, _myid, liter.pos());
      }
    } else  {
      DASH_LOG_TRACE("UnorderedMap.insert", "element key lookup");
      iterator found = find(key);
    }
    */

    DASH_LOG_TRACE_VAR("UnorderedMap.insert", found);

    iterator res{};
    if (found != _end) {
      DASH_LOG_TRACE("UnorderedMap.insert", "key found");
      // Existing element found, no insertion:
      res = found;
    } else {
      DASH_LOG_TRACE("UnorderedMap.insert", "key not found");
      // Unit mapped to the new element's key by the hash function:
      DASH_LOG_TRACE("UnorderedMap.insert", "target unit:", unit_bucket.first);
      // No element with specified key exists, insert new value.
      //ts_insert = Timer::Now();
      auto result = do_insert(unit_bucket.first, unit_bucket.second, value);
      res = result.first;
      //d_insert = Timer::ElapsedSince(ts_insert);
    }

    //auto d_exit = Timer::ElapsedSince(ts_enter);

    //DASH_LOG_DEBUG("UnorderedMap.insert(iterator, value)", "elapsed time:", d_exit * 10e-3);
    //DASH_LOG_DEBUG("UnorderedMap.insert(iterator, value)", "elapsed time (find):", d_find * 10e-3);
    //DASH_LOG_DEBUG("UnorderedMap.insert(iterator, value)", "elapsed time (insert_at):", d_insert * 10e-3);
    return res;
  }

  template<class InputIterator>
  void insert(
    // Iterator at first value in the range to insert.
    InputIterator first,
    // Iterator past the last value in the range to insert.
    InputIterator last)
  {
    // TODO: Calling insert() on every single element in the range could cause
    //       multiple calls of globmem.grow(_local_buffer_size).
    //       Could be optimized to allocate additional memory in a single call
    //       of globmem.grow(std::distance(first,last)).
    for (auto it = first; it != last; ++it) {
      insert(*it);
    }
  }

  iterator erase(
    const_iterator position)
  {
    DASH_THROW(
      dash::exception::NotImplemented,
      "dash::UnorderedMap.erase is not implemented.");
  }

  size_type erase(
    /// Key of the container element to remove.
    const key_type & key)
  {
    DASH_THROW(
      dash::exception::NotImplemented,
      "dash::UnorderedMap.erase is not implemented.");
  }

  iterator erase(
    /// Iterator at first element to remove.
    const_iterator first,
    /// Iterator past the last element to remove.
    const_iterator last)
  {
    DASH_THROW(
      dash::exception::NotImplemented,
      "dash::UnorderedMap.erase is not implemented.");
  }

  //////////////////////////////////////////////////////////////////////////
  // Bucket Interface
  //////////////////////////////////////////////////////////////////////////

  inline std::pair<team_unit_t, size_type> bucket(const key_type & key) const
  {
    auto const unit = _key_global_hash(key);
    auto const h = _key_local_hash(key);
    auto const bucket = _rehash_policy.index_for_hash(h);
    return std::make_pair(unit, bucket);
  }

  inline size_type bucket_size(team_unit_t unit, size_type bucket) const
  {
    auto const nunits = _team->size();
    if (unit > (nunits - 1) || bucket > _n_local_buckets)
      return size_type(-1);

    auto & u_bucket_cumul_sizes = _local_bucket_cumul_sizes[unit];

    auto b_cumul_size = u_bucket_cumul_sizes[bucket];

    if (bucket > 0 && b_cumul_size > 0) {
        return b_cumul_size - u_bucket_cumul_sizes[bucket - 1];
    }

    return b_cumul_size;
  }

  //////////////////////////////////////////////////////////////////////////
  // Observers
  //////////////////////////////////////////////////////////////////////////

  inline key_equal key_eq() const
  {
    return _key_equal;
  }

  inline global_hasher hash_global() const
  {
    return _key_global_hash;
  }

private:
  /**
   * Helper to resolve address of mapped value from map entries.
   *
   * std::pair cannot be used as MPI data type directly.
   * Offset-to-member only works reliably with offsetof in the general case
   * We have to use `offsetof` as there is no instance of value_type
   * available that could be used to calculate the member offset as
   * `l_ptr_value` is possibly undefined.
   *
   * Using `std::declval()` instead (to generate a compile-time
   * pseudo-instance for member resolution) only works if Key and Mapped
   * are default-constructible.
   *
   * Finally, the distance obtained from
   *
   *   &(lptr_value->second) - lptr_value
   *
   * had different alignment than the address obtained via offsetof in some
   * cases, depending on the combination of MPI runtime and compiler.
   * Apparently some compilers / standard libs have special treatment
   * (padding?) for members of std::pair such that
   *
   *   __builtin_offsetof(type, member)
   *
   * differs from the member-offset provided by the type system.
   * The alternative, using `offsetof` (resolves to `__builtin_offsetof`
   * automatically if needed) and manual pointer increments works, however.
   */
  void _lptr_value_to_mapped(
    // [IN]  native pointer to map entry
    value_type    * lptr_value,
    // [OUT] corresponding global pointer to mapped value
    dart_gptr_t   & gptr_mapped,
    // [OUT] corresponding native pointer to mapped value
    mapped_type * & lptr_mapped) const
  {
    // Byte offset of mapped value in element type:
    auto mapped_offs = offsetof(value_type, second);
    DASH_LOG_TRACE("UnorderedMap.lptr_value_to_mapped()",
                   "byte offset of mapped member:", mapped_offs);
    // Increment pointers to element by byte offset of mapped value member:
    if (lptr_value != nullptr) {
        if (std::is_standard_layout<value_type>::value) {
        // Convert to char pointer for byte-wise increment:
        char * b_lptr_mapped = reinterpret_cast<char *>(lptr_value);
        b_lptr_mapped       += mapped_offs;
        // Convert to mapped type pointer:
        lptr_mapped          = reinterpret_cast<mapped_type *>(b_lptr_mapped);
      } else {
        lptr_mapped = &(lptr_value->second);
      }
    }
    if (!DART_GPTR_ISNULL(gptr_mapped)) {
      DASH_ASSERT_RETURNS(
        dart_gptr_incaddr(&gptr_mapped, mapped_offs),
        DART_OK);
    }
    DASH_LOG_TRACE("UnorderedMap.lptr_value_to_mapped >",
                   "gptr to mapped:", gptr_mapped);
    DASH_LOG_TRACE("UnorderedMap.lptr_value_to_mapped >",
                   "lptr to mapped:", lptr_mapped);
  }


  /**
   * Insert value at specified unit.
   */
  std::pair<iterator, bool> do_insert(
    /// target unit
    team_unit_t        unit,
    /// bucket index
    size_type     bucket_idx,
    /// The element to insert.
    const value_type & value)
  {
    DASH_LOG_TRACE("UnorderedMap.do_insert()",
                   "unit:",   unit,
                   "key:",    value.first);
    auto result = std::make_pair(_end, false);

    if (unit != _myid) {
      //TODO rko
      DASH_THROW(dash::exception::NotImplemented, "dash::UnorderedMap : Adding elements to a remote unit not supported yet!");
    }


    auto bkt_size_gptr = _local_bucket_sizes[unit * _n_local_buckets + bucket_idx].dart_gptr();
    auto old_bucket_size = GlobRef<Atomic<size_type>>(bkt_size_gptr).fetch_add(1);

    size_type local_capacity   = _globmem->local_size();

    // Pointer to new element:
    value_type * lptr_insert = nullptr;

    if (old_bucket_size >= _local_buffer_size) {
      //TODO rko
      DASH_THROW(dash::exception::NotImplemented, "dash::UnorderedMap : Dynamic groth not supported yet!");
    };

    //TODO rko: we should only increment if we do really the insert
    size_type old_local_size   = GlobRef<Atomic<size_type>>(
                                    _local_size_gptr
                                 ).fetch_add(1);

    auto const new_local_size = old_local_size + 1;

    auto const new_bucket_size = old_bucket_size + 1;
    auto const idx_insert = bucket_idx * _local_buffer_size + old_bucket_size;

    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", local_capacity);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", _local_buffer_size);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", old_local_size);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", new_local_size);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", _local_cumul_sizes[_myid]);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", old_bucket_size);

    // Acquire target pointer of new element:
    if (new_bucket_size > _local_buffer_size) {
      DASH_LOG_TRACE("UnorderedMap.do_insert",
                     "globmem.grow(", _local_buffer_size, ")");
      lptr_insert = static_cast<value_type *>(
                      _globmem->grow(_local_buffer_size));
    } else {
      lptr_insert = static_cast<value_type *>(
                      _globmem->lbegin() + idx_insert);
    }
    // Assign new value to insert position.
    DASH_LOG_TRACE("UnorderedMap.do_insert", "value target address:",
                   lptr_insert);
    DASH_ASSERT(lptr_insert != nullptr);
    // Using placement new to avoid assignment/copy as value_type is
    // const:
    // TODO rko: Call Construct of the underlying Allocator
    new (lptr_insert) value_type(value);

    //increment local cumul sizes
    _local_cumul_sizes[_myid] += 1;

    auto add1 = [](size_type val) { return val + 1;};
    std::transform(_local_cumul_sizes.begin() + _myid.id + 1,
                   _local_cumul_sizes.end(),
                   _local_cumul_sizes.begin() + _myid.id  + 1,
                   add1);

    //increment local bucket cumul sizes of this unit
    auto & u_bkt_cumul_sizes = _local_bucket_cumul_sizes[unit];

    if (u_bkt_cumul_sizes.size() <= bucket_idx)
      u_bkt_cumul_sizes.resize(bucket_idx + 1);


    u_bkt_cumul_sizes[bucket_idx] += 1;

    auto fst_bkt_after = u_bkt_cumul_sizes.begin();
    std::advance(fst_bkt_after, bucket_idx + 1);
    std::transform(fst_bkt_after, u_bkt_cumul_sizes.end(), fst_bkt_after, add1);

    // Convert local iterator to global iterator:
    DASH_LOG_TRACE("UnorderedMap.do_insert", "converting to global iterator",
                   "unit:", unit, "lidx:", old_local_size);
    result.first = iterator(this, unit, idx_insert);
    result.second = true;

    if (unit != _myid) {
      DASH_LOG_TRACE("UnorderedMap.insert", "remote insertion");
      // Mark inserted element for move to remote unit in next commit:
      _move_elements.push_back(result.first);
    } else {
      //Update local end
      ++_lend;
    }

    // Update iterators as global memory space has been changed for the
    // active unit:
    auto new_size = size();
    DASH_LOG_TRACE("UnorderedMap.do_insert", "new size:", new_size);
    DASH_LOG_TRACE("UnorderedMap.do_insert", "updating _begin");
    _begin        = iterator(this, 0);
    DASH_LOG_TRACE("UnorderedMap.do_insert", "updating _end");
    _end          = iterator(this, new_size);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", _begin);
    DASH_LOG_TRACE_VAR("UnorderedMap.do_insert", _end);
    DASH_LOG_DEBUG("UnorderedMap.do_insert >",
                   (result.second ? "inserted" : "existing"), ":",
                   result.first);
    return result;
  }

  const_iterator do_find(team_unit_t unit, index_type bucket, const key_type & key) const {

    //get the logical index of this bucket which is the number of elements
    //residing in the buckets before this (!) bucket

    auto & u_cumul_bucket_sizes = _local_bucket_cumul_sizes[unit];

    if (bucket >= u_cumul_bucket_sizes.size()) return const_iterator{_end};

    index_type bucket_lidx = (bucket > 0) ? _local_bucket_cumul_sizes[unit][bucket -1] : 0;
    index_type bucket_end = _local_bucket_cumul_sizes[unit][bucket];

    const_iterator null{};
    const_iterator found{};

    if (unit == _myid) {
      // Local search
      local_iterator lstart{const_cast<self_t *>(this), bucket_lidx};
      local_iterator lend{const_cast<self_t *>(this), bucket_end};

      const_local_iterator liter =
          std::find_if(lstart, lend, [&](const value_type & v) {
            return _key_equal(v.first, key);
          });

      if (liter != lend) {
        auto lpos = liter.lpos();
        DASH_ASSERT(lpos.unit == unit);
        found =
            const_iterator{const_cast<self_t *>(this), lpos.unit, lpos.index};
      }
    } else {
      //Global Search
      /*
      iterator start{this, unit, bucket_lidx};
      iterator end{this, unit, _local_bucket_cumul_sizes[unit][bucket]};

      const_iterator g_found = std::find_if(
                               start, end,
                               [&](const_iterator & v) {
                                 return _key_equal((*v), key);
                               });
      */
      DASH_THROW(dash::exception::NotImplemented, "global map iterator search");
      //if (g_found != end) {
        found = _end;//g_found;
      //}
    }

    return (found == null) ? const_iterator{_end} : found;
  }

}; // class UnorderedMap

#endif // ifndef DOXYGEN

} // namespace dash

#endif // DASH__MAP__UNORDERED_MAP_H__INCLUDED