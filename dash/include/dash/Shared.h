#ifndef DASH__SHARED_H_
#define DASH__SHARED_H_

#include <dash/dart/if/dart_types.h>

#include <dash/memory/GlobStaticMem.h>
#include <dash/GlobRef.h>
#include <dash/Allocator.h>

#include <dash/iterator/GlobIter.h>

#include <memory>


  namespace dash {

  /**
   * Shared access to a value in global memory across a team.
   *
   * \tparam  ElementType  The type of the shared value.
   */
  template<typename ElementType>
  class Shared {
  private:
    typedef Shared<ElementType>   self_t;
    typedef ElementType           element_t;

  public:
    typedef size_t                                      size_type;
    typedef size_t                                difference_type;

    typedef GlobRef<      element_t>                    reference;
    typedef GlobRef<const element_t>              const_reference;

    // Note: For dash::Shared<dash::Atomic<T>>, reference type is
  //       dash::GlobRef<dash::Atomic<T>> with value_type T.
  //
  //       See definition of dash::GlobRef in dash/atomic/GlobAtomicRef.h.
  typedef typename reference::value_type             value_type;

private:
  typedef dash::GlobStaticMem<
            value_type,
            dash::allocator::LocalAllocator<value_type> >
          GlobMem_t;

  template<typename T_>
  friend void swap(Shared<T_> & a, Shared<T_> & b);

private:
  dash::Team                   * _team       = nullptr;
  team_unit_t                    _owner;
  std::shared_ptr<GlobMem_t>     _globmem    = nullptr;
  dart_gptr_t                    _dart_gptr  = DART_GPTR_NULL;

  bool                           _is_initialized = false;

public:
  /**
   * Constructor, allocates shared value at single unit in specified team.
   */
  Shared(
    /// Unit id of the shared value's owner.
    team_unit_t   owner = team_unit_t(0),
    /// Team containing all units accessing the element in shared memory
    Team     &    team  = dash::Team::All())
    : _team(&team),
      _owner(owner),
      _dart_gptr(DART_GPTR_NULL)
  {
    DASH_LOG_DEBUG_VAR("Shared.Shared(team,owner)", owner);
    if (dash::is_initialized()) {
      _allocate();
    } else {
      DASH_LOG_DEBUG("Shared.Shared(team, owner)", "DASH is not initialized --> explicit call to Shared.allocate(owner, team) needed");
    }
    DASH_LOG_DEBUG_VAR("Shared.Shared(team,owner) >", _dart_gptr);
  }


  /**
   * Destructor, frees shared memory.
   */
  ~Shared()
  {
    DASH_LOG_DEBUG("Shared.~Shared()");
    _globmem.reset();
    DASH_LOG_DEBUG("Shared.~Shared >");
  }

  /**
   * Copy-constructor.
   */
  Shared(const self_t & other) = default;

  /**
   * Move-constructor. Transfers ownership from other instance.
   */
  Shared(self_t && other) = default;

  /**
   * Assignment operator.
   */
  self_t & operator=(const self_t & other) = default;

  /**
   * Move-assignment operator.
   */
  self_t & operator=(self_t && other) = default;

  /**
   * Set the value of the shared element.
   */
  void set(value_type val)
  {
    DASH_LOG_DEBUG_VAR("Shared.set()", val);
    DASH_LOG_DEBUG_VAR("Shared.set",   _owner);
    DASH_LOG_DEBUG_VAR("Shared.set",   _dart_gptr);
    DASH_ASSERT(!DART_GPTR_ISNULL(_dart_gptr));
    this->get().set(val);
    DASH_LOG_DEBUG("Shared.set >");
  }

  /**
   * Get a reference on the shared value.
   */
  reference get()
  {
    DASH_LOG_DEBUG("Shared.cget()");
    DASH_LOG_DEBUG_VAR("Shared.cget", _owner);
    DASH_LOG_DEBUG_VAR("Shared.get", _dart_gptr);
    DASH_ASSERT(!DART_GPTR_ISNULL(_dart_gptr));
    return reference(_dart_gptr);
  }

  /**
   * Get a const reference on the shared value.
   */
  const_reference get() const
  {
    DASH_LOG_DEBUG("Shared.get()");
    DASH_LOG_DEBUG_VAR("Shared.get", _owner);
    DASH_LOG_DEBUG_VAR("Shared.get", _dart_gptr);
    DASH_ASSERT(!DART_GPTR_ISNULL(_dart_gptr));
    return const_reference(_dart_gptr);
  }

  /**
   * Flush global memory of shared value.
   */
  void flush()
  {
    DASH_ASSERT(!DART_GPTR_ISNULL(_dart_gptr));
    DASH_ASSERT_RETURNS(
      dart_flush(_dart_gptr),
      DART_OK);
  }

  /**
   * Flush global memory of shared value and synchronize its associated
   * units.
   */
  void barrier()
  {
    flush();
    DASH_ASSERT(_team != nullptr);
    _team->barrier();
  }

  /**
   * Get underlying DART global pointer of the shared variable.
   */
  inline dart_gptr_t dart_gptr() const noexcept
  {
    return _dart_gptr;
  }

  bool allocate(
      /// Unit id of the shared value's owner.
      team_unit_t owner = team_unit_t(0),
      /// Team containing all units accessing the element in shared memory
      Team & team = dash::Team::All())
  {
    DASH_LOG_DEBUG("Shared.allocate(team,owner)");
    bool res = false;
    if (_is_initialized) {
      DASH_LOG_ERROR(
          "Shared.allocate(team,owner)", "Shared Value is already allocated");
    }
    else {
      _team  = &team;
      _owner = owner;
      res = _allocate();
    }
    DASH_LOG_DEBUG("Shared.allocate(team,owner) >");
    return res;
  }

private:
  bool _allocate()
  {
    DASH_ASSERT(_team);

    bool res = false;

    if (_team->myid() == _owner) {
      DASH_LOG_DEBUG(
          "Shared.Shared(team,owner)",
          "allocating shared value in local memory");
      _globmem   = std::make_shared<GlobMem_t>(1, *_team);
      _dart_gptr = _globmem->begin().dart_gptr();
    }
    // Broadcast global pointer of shared value at unit 0 to all units:
    dash::dart_storage<dart_gptr_t> ds(1);
    int ret = dart_bcast(&_dart_gptr, ds.nelem, ds.dtype, _owner, _team->dart_id());
    res = DART_OK == ret;
    _is_initialized = true;

    return res;
  }

};

template<typename T>
void swap(
  dash::Shared<T> & a,
  dash::Shared<T> & b)
{
  using std::swap;
  swap(a._team,       b._team);
  swap(a._owner,      b._owner);
  swap(a._globmem,    b._globmem);
  swap(a._dart_gptr,  b._dart_gptr);
}

} // namespace dash

#endif // DASH__SHARED_H_
