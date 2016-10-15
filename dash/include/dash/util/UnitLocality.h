#ifndef DASH__UTIL__UNIT_LOCALITY_H__INCLUDED
#define DASH__UTIL__UNIT_LOCALITY_H__INCLUDED

#include <dash/util/Locality.h>
#include <dash/util/LocalityDomain.h>
#include <dash/util/Config.h>

#include <dash/algorithm/internal/String.h>

#include <dash/dart/if/dart_types.h>
#include <dash/dart/if/dart_locality.h>

#include <dash/Exception.h>
#include <dash/Team.h>

#include <string>
#include <vector>
#include <unordered_map>
#include <utility>
#include <iterator>
#include <algorithm>


namespace dash {
namespace util {

/**
 * Wrapper of a single \c dart_unit_locality_t object.
 */
class UnitLocality
{
private:
  typedef UnitLocality  self_t;

public:

  UnitLocality(
    dash::Team  & team,
    dart_unit_t   unit)
  : _team(&team)
  {
    DASH_ASSERT_RETURNS(
      dart_unit_locality(
        team.dart_id(), unit, &_unit_locality),
      DART_OK);
  }

  UnitLocality()                                 = default;
  UnitLocality(const UnitLocality &)             = default;
  UnitLocality & operator=(const UnitLocality &) = default;

  inline const dart_hwinfo_t & hwinfo() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
    return _unit_locality->hwinfo;
  }

  inline dart_hwinfo_t & hwinfo()
  {
    DASH_ASSERT(nullptr != _unit_locality);
    return _unit_locality->hwinfo;
  }

  inline dash::Team & team()
  {
    if (nullptr == _team) {
      return dash::Team::Null();
    }
    return *_team;
  }

  inline dart_unit_t unit_id() const
  {
    return nullptr == _unit_locality
           ? DART_UNDEFINED_UNIT_ID
           : _unit_locality->unit;
  }

  inline std::string domain_tag() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
//  return _unit_locality->domain_tag;
    return _unit_locality->domain.domain_tag;
  }

  inline std::string host() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
//  return _unit_locality->host;
    return _unit_locality->hwinfo.host;
  }

  inline void set_domain_tag(
    const std::string & tag)
  {
//  strcpy(_unit_locality->domain_tag, tag.c_str());
    strcpy(_unit_locality->domain.domain_tag, tag.c_str());
  }

  inline void set_host(
    const std::string & hostname)
  {
//  strcpy(_unit_locality->host, hostname.c_str());
    strcpy(_unit_locality->hwinfo.host, hostname.c_str());
  }

  inline int num_cores() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
    return (_unit_locality->hwinfo.num_cores);
  }

  inline int min_threads()
  {
    return (_unit_locality == nullptr)
           ? -1 : std::max<int>(_unit_locality->hwinfo.min_threads, 1);
  }

  inline int max_threads()
  {
    return (_unit_locality == nullptr)
           ? -1 : std::max<int>(_unit_locality->hwinfo.max_threads, 1);
  }

  inline int num_threads() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
    return (dash::util::Config::get<bool>("DASH_MAX_SMT")
               ? _unit_locality->hwinfo.max_threads
               : _unit_locality->hwinfo.min_threads);
  }

  inline int num_numa() const
  {
    dart_domain_locality_t * dom = &_unit_locality->domain;
    while (dom->scope >= DART_LOCALITY_SCOPE_NUMA) {
      dom = dom->parent;
    }
    return dom->num_domains;
  }

  inline int cpu_mhz() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
    return (_unit_locality->hwinfo.max_cpu_mhz);
  }

  inline int max_shmem_mbps() const
  {
    DASH_ASSERT(nullptr != _unit_locality);
    return (_unit_locality->hwinfo.max_shmem_mbps);
  }

  /**
   * Number of threads currently available to the active unit.
   *
   * The returned value is calculated from unit locality data and hardware
   * specifications and can, for example, be used to set the \c num_threads
   * parameter of OpenMP sections:
   *
   * \code
   * #ifdef DASH_ENABLE_OPENMP
   *   auto n_threads = dash::util::Locality::NumUnitDomainThreads();
   *   if (n_threads > 1) {
   *     #pragma omp parallel num_threads(n_threads) private(t_id)
   *     {
   *        // ...
   *     }
   * #endif
   * \endcode
   *
   * The following configuration keys affect the number of available
   * threads:
   *
   * - <tt>DASH_DISABLE_THREADS</tt>:
   *   If set, disables multi-threading at unit scope and this method
   *   returns 1.
   * - <tt>DASH_MAX_SMT</tt>:
   *   If set, virtual SMT CPUs (hyperthreads) instead of physical cores
   *   are used to determine availble threads.
   * - <tt>DASH_MAX_UNIT_THREADS</tt>:
   *   Specifies the maximum number of threads available to a single
   *   unit.
   *
   * Note that these settings may differ between hosts.
   *
   * Example for MPI:
   *
   * <tt>
   * mpirun -host node.0 -env DASH_MAX_UNIT_THREADS 4 -n 16 myprogram
   *      : -host node.1 -env DASH_MAX_UNIT_THREADS 2 -n 32 myprogram
   * </tt>
   *
   * The DASH configuration can also be changed at run time with the
   * \c dash::util::Config interface.
   *
   * \see dash::util::Config
   * \see dash::util::TeamLocality
   *
   */
  inline int num_domain_threads()
  {
    auto n_threads = num_cores();
    if (dash::util::Config::get<bool>("DASH_DISABLE_THREADS")) {
      // Threads disabled in unit scope:
      n_threads  = 1;
    } else if (dash::util::Config::get<bool>("DASH_MAX_SMT")) {
      // Configured to use SMT (hyperthreads):
      n_threads *= max_threads();
    } else {
      // Start one thread on every physical core assigned to this unit:
      n_threads *= min_threads();
    }
    if (dash::util::Config::is_set("DASH_MAX_UNIT_THREADS")) {
      n_threads  = std::min(dash::util::Config::get<int>(
                              "DASH_MAX_UNIT_THREADS"),
                            n_threads);
    }
    return n_threads;
  }

private:

  dash::Team             * _team          = nullptr;
  dart_unit_locality_t   * _unit_locality = nullptr;

}; // class UnitLocality

} // namespace util
} // namespace dash

#endif // DASH__UTIL__UNIT_LOCALITY_H__INCLUDED
