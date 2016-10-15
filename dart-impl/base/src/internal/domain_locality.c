/**
 * \file dash/dart/base/internal/domain_locality.c
 */
#include <dash/dart/base/macro.h>
#include <dash/dart/base/logging.h>
#include <dash/dart/base/locality.h>
#include <dash/dart/base/assert.h>
#include <dash/dart/base/hwinfo.h>

#include <dash/dart/base/string.h>
#include <dash/dart/base/array.h>
#include <dash/dart/base/math.h>
#include <dash/dart/base/internal/host_topology.h>
#include <dash/dart/base/internal/unit_locality.h>
#include <dash/dart/base/internal/domain_locality.h>

#include <dash/dart/if/dart_types.h>
#include <dash/dart/if/dart_locality.h>

#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sched.h>
#include <limits.h>

/* ======================================================================== *
 * Private Functions: Declarations                                          *
 * ======================================================================== */

dart_ret_t dart__base__locality__domain__create_global_subdomain(
  dart_host_topology_t   * host_topology,
  dart_unit_mapping_t    * unit_mapping,
  dart_domain_locality_t * global_domain,
  dart_domain_locality_t * subdomain,
  int                      rel_idx);

dart_ret_t dart__base__locality__domain__create_node_subdomain(
  dart_host_topology_t   * host_topology,
  dart_unit_mapping_t    * unit_mapping,
  dart_domain_locality_t * node_domain,
  dart_domain_locality_t * subdomain,
  int                      rel_idx);

dart_ret_t dart__base__locality__domain__create_module_subdomain(
  dart_host_topology_t   * host_topology,
  dart_unit_mapping_t    * unit_mapping,
  dart_domain_locality_t * module_domain,
  dart_domain_locality_t * subdomain,
  int                      rel_idx);

dart_ret_t dart__base__locality__domain__create_numa_subdomain(
  dart_host_topology_t   * host_topology,
  dart_unit_mapping_t    * unit_mapping,
  dart_domain_locality_t * numa_domain,
  dart_domain_locality_t * subdomain,
  int                      rel_idx);

dart_ret_t dart__base__locality__domain__create_package_subdomain(
  dart_host_topology_t   * host_topology,
  dart_unit_mapping_t    * unit_mapping,
  dart_domain_locality_t * numa_domain,
  dart_domain_locality_t * subdomain,
  int                      rel_idx);

dart_ret_t dart__base__locality__domain__create_cache_subdomain(
  dart_host_topology_t   * host_topology,
  dart_unit_mapping_t    * unit_mapping,
  dart_domain_locality_t * numa_domain,
  dart_domain_locality_t * subdomain,
  int                      rel_idx);

/* ===================================================================== *
 * Internal Functions                                                    *
 * ===================================================================== */

dart_ret_t dart__base__locality__domain__init(
  dart_domain_locality_t * loc)
{
  DART_LOG_TRACE("dart__base__locality__domain_locality_init() loc: %p",
                 (void *)loc);
  if (loc == NULL) {
    DART_LOG_ERROR("dart__base__locality__domain_locality_init ! null");
    return DART_ERR_INVAL;
  }
  loc->domain_tag[0]  = '\0';
//loc->host[0]        = '\0';
  loc->scope          = DART_LOCALITY_SCOPE_UNDEFINED;
  loc->level          = 0;
  loc->relative_index = 0;
  loc->parent         = NULL;
  loc->num_domains    = 0;
  loc->domains        = NULL;
  loc->num_nodes      = -1;
  loc->num_units      = -1;

  DART_LOG_TRACE("dart__base__locality__domain_locality_init >");
  return DART_OK;
}

dart_ret_t dart__base__locality__domain__destruct(
  dart_domain_locality_t * domain)
{
  DART_LOG_DEBUG("dart__base__locality__domain__destruct() "
                 "domain(%p)", (void *)domain);

  dart_ret_t ret = DART_OK;

  if (domain == NULL) {
    DART_LOG_DEBUG("dart__base__locality__domain__destruct > domain NULL");
    return DART_OK;
  }

  DART_LOG_DEBUG("dart__base__locality__domain__destruct :   "
                 "domain tag: %s level: %d",
                 domain->domain_tag, domain->level);

  if (domain->num_domains > 0 && NULL == domain->domains) {
    DART_LOG_ERROR("dart__base__locality__domain__destruct ! "
                   "domain.domains must not be NULL for "
                   "domain.num_domains = %d", domain->num_domains);
    return DART_ERR_INVAL;
  }
  else if (domain->num_domains == 0 && NULL != domain->domains) {
    DART_LOG_ERROR("dart__base__locality__domain__destruct ! "
                   "domain.domains expected to be NULL for "
                   "domain.num_domains = %d", domain->num_domains);
//  return DART_ERR_INVAL;
  }

  /* deallocate child nodes in depth-first recursion: */
  for (int subdom_idx = 0; subdom_idx < domain->num_domains; ++subdom_idx) {
    ret = dart__base__locality__domain__destruct(
            domain->domains + subdom_idx);
    if (DART_OK != ret) {
      return ret;
    }
  }
  /* deallocate node itself: */
  if (NULL != domain->domains) {
    free(domain->domains);
    domain->domains  = NULL;
  }
  if (NULL != domain->unit_ids) {
    free(domain->unit_ids);
    domain->unit_ids = NULL;
  }
  domain->num_domains = 0;
  domain->num_units   = 0;

  DART_LOG_DEBUG("dart__base__locality__domain__destruct > "
                 "domain(%p)", (void *)domain);
  return DART_OK;
}

dart_ret_t dart__base__locality__domain__copy(
  const dart_domain_locality_t   * domain_src,
  dart_domain_locality_t         * domain_dst)
{
  DART_LOG_TRACE("dart__base__locality__domain__copy() "
                 "domain: %s (level %d) subdomains: %d units: %d (%p = %p)",
                 domain_src->domain_tag,  domain_src->level,
                 domain_src->num_domains, domain_src->num_units,
                 (void *)domain_dst, (void *)domain_src);
  dart_ret_t ret;

  *domain_dst = *domain_src;

  /* Copy unit ids:
   */
  if (domain_src->num_units > 0) {
    if (NULL == domain_src->unit_ids) {
      DART_LOG_ERROR("dart__base__locality__domain__copy: domain %s "
                     "has num_units = %d but domain->unit_ids is NULL",
                     domain_src->domain_tag, domain_src->num_units);
      return DART_ERR_OTHER;
    }
    domain_dst->unit_ids = malloc(sizeof(dart_unit_t) *
                                  domain_src->num_units);
    for (int u = 0; u < domain_src->num_units; u++) {
      domain_dst->unit_ids[u] = domain_src->unit_ids[u];
    }
  } else {
    if (NULL != domain_src->unit_ids) {
      DART_LOG_ERROR("dart__base__locality__domain__copy: domain %s "
                     "has num_units = %d, expected unit_ids == NULL",
                     domain_src->domain_tag, domain_src->num_units);
      return DART_ERR_OTHER;
    }
    domain_dst->unit_ids = NULL;
  }

  /* Copy subdomains:
   */
  if (domain_src->num_domains > 0) {
    if (NULL == domain_src->domains) {
      DART_LOG_ERROR("dart__base__locality__domain__copy: domain %s "
                     "has num_domains = %d, expected domains != NULL",
                     domain_src->domain_tag, domain_src->num_domains);
      return DART_ERR_OTHER;
    }
    domain_dst->domains = malloc(sizeof(dart_domain_locality_t) *
                                 domain_src->num_domains);
  } else {
    if (NULL != domain_src->domains) {
      DART_LOG_ERROR("dart__base__locality__domain__copy: domain %s "
                     "has num_domains = %d, expected domains = NULL",
                     domain_src->domain_tag, domain_src->num_domains);
//    return DART_ERR_OTHER;
    }
    domain_dst->domains = NULL;
  }

  /* Recursively copy subdomains:
   */
  for (int sd = 0; sd < domain_src->num_domains; sd++) {
    const dart_domain_locality_t * subdomain_src = domain_src->domains + sd;
    dart_domain_locality_t * subdomain_dst       = domain_dst->domains + sd;

    ret = dart__base__locality__domain__copy(
            subdomain_src,
            subdomain_dst);
    if (ret != DART_OK) {
      return ret;
    }
    domain_dst->domains[sd].parent = domain_dst;
  }

  DART_LOG_TRACE("dart__base__locality__domain__copy >");
  return DART_OK;
}

dart_ret_t dart__base__locality__domain__update_subdomains(
  dart_domain_locality_t         * domain)
{
  int is_unit_scope = ((int)domain->scope >= (int)DART_LOCALITY_SCOPE_UNIT);
  DART_LOG_TRACE("dart__base__locality__domain__update_subdomains() "
                 "domain: %s, scope: %d, subdomains: %d, units: %d, "
                 "in unit scope: %s",
                 domain->domain_tag,  domain->scope,
                 domain->num_domains, domain->num_units,
                 (is_unit_scope ? "true" : "false"));
  if (is_unit_scope) {
//  DART_ASSERT(domain->num_units   <= 1);
    // TODO: Check if there is a valid scenario for num_domains > 0 at
    //       unit scope:
//  DART_ASSERT(domain->num_domains == 0);
  } else {
    domain->num_units = 0;
  }
  for (int sd = 0; sd < domain->num_domains; sd++) {
    dart_domain_locality_t * subdomain = domain->domains + sd;
    subdomain->team           = domain->team;
    subdomain->level          = domain->level + 1;
    subdomain->relative_index = sd;
    subdomain->parent         = domain;

    sprintf(subdomain->domain_tag, "%s.%d", domain->domain_tag, sd);

    /* Recursively update subdomains: */
    DART_LOG_TRACE("dart__base__locality__domain__update_subdomains --v");
    dart__base__locality__domain__update_subdomains(
      subdomain);
    DART_LOG_TRACE("dart__base__locality__domain__update_subdomains --^");

    if (!is_unit_scope) {
      domain->num_units += subdomain->num_units;
    }
  }
  if (domain->num_units   == 0 && domain->unit_ids != NULL) {
    free(domain->unit_ids);
  }
  if (domain->num_domains == 0 && domain->domains  != NULL) {
    free(domain->domains);
  }
  if (domain->num_units > 0) {
    if (is_unit_scope) {
      dart_unit_t unit_id = domain->unit_ids[0];
      free(domain->unit_ids);
      domain->unit_ids    = malloc(sizeof(dart_unit_t));
      domain->unit_ids[0] = unit_id;
    } else {
      domain->unit_ids    = malloc(sizeof(dart_unit_t) * domain->num_units);
      int domain_unit_idx = 0;
      for (int sd = 0; sd < domain->num_domains; sd++) {
        dart_domain_locality_t * subdomain = domain->domains + sd;
        memcpy(domain->unit_ids + domain_unit_idx,
               subdomain->unit_ids,
               sizeof(dart_unit_t) * subdomain->num_units);
        domain_unit_idx += subdomain->num_units;
      }
    }
  } else {
    domain->unit_ids = NULL;
  }
  DART_LOG_TRACE("dart__base__locality__domain__update_subdomains > "
                 "domain: %s, scope: %d, subdomains: %d, units: %d",
                 domain->domain_tag,  domain->scope,
                 domain->num_domains, domain->num_units);
  return DART_OK;
}

/**
 * Find subdomain at arbitrary level below a specified domain.
 */
dart_ret_t dart__base__locality__domain__child(
  const dart_domain_locality_t   * domain,
  const char                     * subdomain_tag,
  dart_domain_locality_t        ** subdomain_out)
{
  if (strcmp(domain->domain_tag, subdomain_tag) == 0) {
    *subdomain_out = (dart_domain_locality_t *)(domain);
    return DART_OK;
  }
  /*
   * TODO: Optimize, currently using exhaustive search.
   */
  for (int sd = 0; sd < domain->num_domains; ++sd) {
    if (dart__base__locality__domain__child(
          &domain->domains[sd], subdomain_tag, subdomain_out)
        == DART_OK) {
      return DART_OK;
    }
  }
  return DART_ERR_NOTFOUND;
}

/**
 * Find common parent of specified domains.
 */
dart_ret_t dart__base__locality__domain__parent(
  const dart_domain_locality_t   * domain_in,
  const char                    ** subdomain_tags,
  int                              num_subdomain_tags,
  dart_domain_locality_t        ** domain_out)
{
  *domain_out = NULL;

  /* Parent domain tag of subdomains is common prefix of subdomain tags:
   */
  char subdomains_prefix[DART_LOCALITY_DOMAIN_TAG_MAX_SIZE];
  int  subdomains_prefix_len = dart__base__strscommonprefix(
                                 subdomain_tags,
                                 num_subdomain_tags,
                                 subdomains_prefix);
  /* Remove trailing '.' */
  if (subdomains_prefix[subdomains_prefix_len - 1] == '.') {
    subdomains_prefix_len--;
    subdomains_prefix[subdomains_prefix_len] = '\0';
  }
  if (subdomains_prefix_len == 0) {
    *domain_out = (dart_domain_locality_t *)(domain_in);
    return DART_OK;
  }

  /* Find domain tagged with subdomains prefix: */
  return dart__base__locality__domain__child(
           domain_in, subdomains_prefix, domain_out);
}

/**
 * Remove all child nodes from a domain that do not match the specified
 * domain tags.
 */
dart_ret_t dart__base__locality__domain__filter_subdomains_if(
  dart_domain_locality_t         * domain,
  dart_domain_predicate_t          pred)
{
  dart__unused(domain);
  dart__unused(pred);

  return DART_OK;
}

/**
 * Remove all child nodes from a domain that match or do not match the
 * specified domain tags.
 */
dart_ret_t dart__base__locality__domain__filter_subdomains(
  dart_domain_locality_t         * domain,
  const char                    ** subdomain_tags,
  int                              num_subdomain_tags,
  int                              remove_matches)
{
  dart_ret_t ret;

  int is_unit_scope = ((int)domain->scope >= (int)DART_LOCALITY_SCOPE_UNIT);
  int matched       = 0;
  int unit_idx      = 0;
  int subdomain_idx = 0;
//int num_numa      = 0;

  DART_LOG_TRACE("dart__base__locality__domain__filter_subdomains() "
                 "domain: %s, level: %d, domains: %d, units: %d",
                 domain->domain_tag,  domain->level,
                 domain->num_domains, domain->num_units);

  if (is_unit_scope) {
    return DART_OK;
  }

  for (int sd = 0; sd < domain->num_domains; sd++) {
    /* ---------------------------------------------------------------- *
     * Selection predicate is just this block.                          *
     * Could use functors to allow arbitrary selection functions.       *
     *                                                                  */
    char * subdomain_tag     = domain->domains[sd].domain_tag;
    size_t subdomain_tag_len = strlen(subdomain_tag);
    matched = 0;
    for (int dt = 0; dt < num_subdomain_tags; dt++) {
      size_t filter_tag_len    = strlen(subdomain_tags[dt]);
      size_t common_prefix_len = dart__base__strcommonprefix(
                                   subdomain_tag, subdomain_tags[dt], NULL);
      /* When removing matches: Match domains with full domain tag filter in
       * prefix, e.g. ".0.1" matches ".0.1.0"
       * -> minimum match length is length of filter tag
       * When selecting matches: Match domain tags that are fully included
       * in filter tag
       * -> minimum match length is length of subdomain:
       */
      size_t min_tag_match_len = (remove_matches == 1)
                                 ? filter_tag_len
                                 : subdomain_tag_len;

      if (common_prefix_len >= min_tag_match_len) {
        matched = 1;
        break;
      }
    }
    /*                                                                   *
     * ----------------------------------------------------------------- */

    if (matched == remove_matches) {
      continue;
    }
    DART_LOG_TRACE("dart__base__locality__domain__filter_subdomains : "
                   "  --v  subdomain[%d] = %s matched", sd, subdomain_tag);

    if (subdomain_idx != sd) {
      memcpy(domain->domains + subdomain_idx,
             domain->domains + sd,
             sizeof(dart_domain_locality_t));
      domain->domains[subdomain_idx].relative_index = subdomain_idx;
    }

    ret = dart__base__locality__domain__filter_subdomains(
            domain->domains + subdomain_idx,
            subdomain_tags,
            num_subdomain_tags,
            remove_matches);
    if (ret != DART_OK) {
      return ret;
    }

    DART_LOG_TRACE("dart__base__locality__domain__filter_subdomains : "
                   "  --^  subdomain[%d] = %s: domains: %d, units: %d", sd,
                   domain->domains[subdomain_idx].domain_tag,
                   domain->domains[subdomain_idx].num_domains,
                   domain->domains[subdomain_idx].num_units);

    /* Collect units and domains bottom-up after maximum recursion
     * depth has been reached:
     */
    if (domain->domains[subdomain_idx].num_units > 0) {
      memcpy(domain->unit_ids + unit_idx,
             domain->domains[subdomain_idx].unit_ids,
             domain->domains[subdomain_idx].num_units *
               sizeof(dart_unit_t));
      unit_idx += domain->domains[subdomain_idx].num_units;
    }
//  num_numa += domain->domains[subdomain_idx].hwinfo.num_numa;
    subdomain_idx++;
  }

  /*
   * Bottom-up accumulation of units and domains:
   */
  DART_LOG_TRACE("dart__base__locality__domain__filter_subdomains : "
                 "--> collected in %s: domains: %d, units: %d",
                 domain->domain_tag, subdomain_idx, unit_idx);

//domain->hwinfo.num_numa = num_numa;

  if (NULL != domain->unit_ids) {
    if (domain->num_units != unit_idx) {
      dart_unit_t * tmp =
        realloc(domain->unit_ids, unit_idx * sizeof(dart_unit_t));
      if (unit_idx == 0) {
        domain->unit_ids = NULL;
      } else if (tmp != NULL) {
        domain->unit_ids = tmp;
      }
      domain->num_units = unit_idx;
    }
  }
  if (NULL != domain->domains) {
    if (domain->num_domains != subdomain_idx) {
      dart_domain_locality_t * tmp =
        realloc(domain->domains,
                subdomain_idx * sizeof(dart_domain_locality_t));
      if (subdomain_idx == 0) {
        domain->domains = NULL;
      } else if (tmp != NULL) {
        domain->domains = tmp;
      }
      domain->num_domains = subdomain_idx;
    }
  }
  DART_LOG_TRACE("dart__base__locality__domain__filter_subdomains >");
  return DART_OK;
}

dart_ret_t dart__base__locality__domain__num_separate_caches(
  dart_domain_locality_t         * domain,
  dart_unit_mapping_t            * unit_mapping,
  int                              cache_level,
  int                           ** cache_ids,
  int                            * num_caches)
{
  DART_LOG_TRACE("dart__base__locality__domain__num_separate_caches: "
                 "domain(rel_idx:%d) "
                 "domain->parent(num_domains:%d unit_ids:%p)",
                 domain->relative_index,
                 domain->parent->num_domains,
                 (void*)(domain->parent->unit_ids));
  if (domain->parent->num_domains < 1 ||
      domain->parent->unit_ids == NULL) {
    return DART_ERR_INVAL;
  }
  int rel_idx       = domain->relative_index;
  int num_dom_units = domain->parent->num_units /
                      domain->parent->num_domains;
  for (int u = 0; u < num_dom_units; ++u) {
    int dom_unit_idx    = (rel_idx * num_dom_units) + u;
    dart_unit_t unit_id = domain->parent->unit_ids[dom_unit_idx];
    /* set host and domain tag of unit in unit locality map: */
    dart_unit_locality_t * unit_loc;
    DART_ASSERT_RETURNS(
      dart__base__unit_locality__at(unit_mapping, unit_id, &unit_loc),
      DART_OK);
    (*cache_ids)[u] = unit_loc->hwinfo.cache_ids[cache_level];
  }

  *num_caches =
    dart__base__intsunique(*cache_ids, num_dom_units);

  if (*num_caches == 1 && (*cache_ids)[0] < 0) {
    *num_caches = 0;
  }

  return DART_OK;
}

/**
 * Recursively initialize subdomains from specified host topology and unit
 * mapping.
 */
dart_ret_t dart__base__locality__domain__create_subdomains(
  dart_domain_locality_t         * domain,
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping)
{
  DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains() "
                 "parent: %p scope: %d level: %d",
                 (void *)domain, domain->scope, domain->level);

  /* ------------------------------------------------------------------- *
   * First step:                                                         *
   *                                                                     *
   *   Determine the number of sub-domains and their scope.              *
   * ------------------------------------------------------------------- */

  dart_locality_scope_t sub_scope;
  sub_scope           = DART_LOCALITY_SCOPE_UNDEFINED;
  const char  * module_hostname;
  const char  * node_hostname;
  dart_unit_t * module_units;
  switch (domain->scope) {
    case DART_LOCALITY_SCOPE_UNDEFINED:
      DART_LOG_ERROR("dart__base__locality__domain__create_subdomains ! "
                     "locality scope undefined");
      return DART_ERR_INVAL;
    case DART_LOCALITY_SCOPE_GLOBAL:
      DART_ASSERT_RETURNS(
        dart__base__host_topology__num_nodes(
          host_topology,
          &domain->num_domains),
        DART_OK);
      sub_scope           = DART_LOCALITY_SCOPE_NODE;
      break;
    case DART_LOCALITY_SCOPE_NODE:
      if (dart__base__host_topology__node(
            host_topology,
            domain->relative_index,
            &node_hostname) != DART_OK) {
        DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains: "
                       "could not get host name of node %d",
                       domain->relative_index);
      } else {
        strncpy(domain->host, node_hostname, DART_LOCALITY_HOST_MAX_SIZE);
        if (dart__base__host_topology__num_node_modules(
              host_topology,
//            domain->host,
              node_hostname,
              &domain->num_domains) != DART_OK) {
          DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains: "
                         "could not get number of modules of host '%s'",
//                       domain->host);
                         node_hostname);
          domain->num_domains = 0;
        }
      }
      sub_scope           = DART_LOCALITY_SCOPE_MODULE;
      break;
    case DART_LOCALITY_SCOPE_MODULE:
//    domain->num_domains = domain->hwinfo.num_numa;
      domain->num_units   = 0;
      module_units        = NULL;
      sub_scope           = DART_LOCALITY_SCOPE_NUMA;
      if (dart__base__host_topology__node(
            host_topology,
            domain->parent->relative_index,
            &node_hostname) != DART_OK) {
        DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains: "
                       "could not get host name of node %d",
                       domain->parent->relative_index);
      } else {
        strncpy(domain->host, node_hostname, DART_LOCALITY_HOST_MAX_SIZE);
        if (dart__base__host_topology__node_module(
              host_topology,
//            domain->host,
              node_hostname,
              domain->relative_index,
              &module_hostname) != DART_OK) {
          DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains: "
                         "could not get module hostname %d of host %s",
                         domain->relative_index,
//                       domain->host);
                         node_hostname);
        } else {
          int num_module_numa_domains;
          if (dart__base__host_topology__module_units(
                host_topology,
                module_hostname,
                &module_units,
                &domain->num_units,
                &num_module_numa_domains) != DART_OK) {
            DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains: "
                           "could not find units of module %s",
                           module_hostname);
          } else {
            domain->num_domains = num_module_numa_domains;
          }
        }
      }
      break;
    case DART_LOCALITY_SCOPE_NUMA:
      if (domain->num_domains <= 0) {
        domain->num_domains = 1;
      }
      sub_scope           = DART_LOCALITY_SCOPE_PACKAGE;
      break;
    case DART_LOCALITY_SCOPE_PACKAGE:
      {
        DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                       "--> PACKAGE scope: "
                       "num_units:%d parent(num_units:%d num_domains:%d)",
                       domain->num_units,
                       domain->parent->num_units,
                       domain->parent->num_domains);
        int num_package_level_caches;
        int * package_level_cache_ids = malloc(domain->num_units *
                                               sizeof(int));
        if (dart__base__locality__domain__num_separate_caches(
              domain, unit_mapping, 2,
              &package_level_cache_ids,
              &num_package_level_caches) != DART_OK) {
          DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains: "
                         "could not get cache in package %d",
                         domain->relative_index);
        } else {
          DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                         "--> PACKAGE scope: "
                         "num_package_caches:%d",
                         num_package_level_caches);
          if (num_package_level_caches > 0) {
            domain->num_domains = num_package_level_caches;
            sub_scope           = DART_LOCALITY_SCOPE_CACHE;
          } else {
            domain->num_domains = domain->num_units;
            sub_scope           = DART_LOCALITY_SCOPE_CORE;
          }
        }
        free(package_level_cache_ids);
      }
      break;
    case DART_LOCALITY_SCOPE_CACHE:
      DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                     "--> CACHE scope: parent.num_units:%d",
                     domain->parent->num_units);
      sub_scope           = DART_LOCALITY_SCOPE_CACHE;
      if (domain->num_domains < 0) {
        domain->num_domains = 1;
      }
      domain->num_units   = domain->parent->num_units /
                            domain->parent->num_domains;
      for (int u = 0; u < domain->num_units; u++) {
        int unit_idx = (domain->num_units * domain->relative_index) + u;
        domain->unit_ids[u] = domain->parent->unit_ids[unit_idx];
      }
      /* domain and its two parent domains are in cache scope, switch
       * to core scope: */
      {
        dart_unit_locality_t * unit_loc;
        /* There always is exactly one unit at cache level: */
//      DART_ASSERT_MSG(
//        domain->num_units == 1,
//        "Expected exactly 1 unit at cache locality scope");
        DART_ASSERT_RETURNS(
          dart__base__unit_locality__at(unit_mapping, domain->unit_ids[0],
                                        &unit_loc),
          DART_OK);
        int cache_level = 3;
        if (domain->scope == DART_LOCALITY_SCOPE_CACHE) {
          cache_level--;
        }
        if (domain->parent->scope == DART_LOCALITY_SCOPE_CACHE) {
          cache_level--;
        }
        if (domain->parent->parent->scope == DART_LOCALITY_SCOPE_CACHE) {
          cache_level--;
        }
//      if (cache_level <= 0 || domain->hwinfo.cache_ids[cache_level] < 0) {
        if (cache_level <= 0 ||
            unit_loc->hwinfo.cache_ids[cache_level] < 0) {
          domain->num_domains = domain->num_units;
          sub_scope           = DART_LOCALITY_SCOPE_CORE;
        }
      }
      break;
    case DART_LOCALITY_SCOPE_CORE:
      domain->num_domains = 0;
      break;
    default:
      DART_LOG_ERROR("dart__base__locality__domain__create_subdomains: "
                     "unknown scope: %d", domain->scope);
      return DART_ERR_INVAL;
  }
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "subdomains: %d", domain->num_domains);
  if (domain->num_domains == 0) {
    domain->domains = NULL;
    DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains > "
                   "domain: %p - scope: %d level: %d "
                   "subdomains: %d domain(%s) - final",
                   (void *)domain, domain->scope, domain->level,
                   domain->num_domains, domain->domain_tag);
    return DART_OK;
  } else {
    /* Allocate subdomains: */
    domain->domains = (dart_domain_locality_t *)(
                         malloc(domain->num_domains *
                                sizeof(dart_domain_locality_t)));
  }

  /* ---------------------------------------------------------------------- *
   * Second step:                                                           *
   *                                                                        *
   *   Now that the number of subdomains is known, initialize the           *
   *   subdomain objects and distribute domain elements like units and      *
   *   cores.                                                               *
   * ---------------------------------------------------------------------- */

  int num_domain_units = 0;
  for (int rel_idx = 0; rel_idx < domain->num_domains; ++rel_idx) {
    DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                   "initialize, level: %d, subdomain %d of %d",
                   domain->level + 1, rel_idx, domain->num_domains);

    dart_domain_locality_t * subdomain = domain->domains + rel_idx;
    dart__base__locality__domain__init(subdomain);

    /* Initialize hwinfo from parent as most properties are identical: */
//  subdomain->hwinfo         = domain->hwinfo;
//  subdomain->hwinfo.num_cores = domain->hwinfo.num_cores;
    subdomain->num_cores      = domain->num_cores;

    subdomain->parent         = domain;
    subdomain->scope          = sub_scope;
    subdomain->relative_index = rel_idx;
    subdomain->level          = domain->level + 1;
    subdomain->team           = domain->team;
//  subdomain->node_id        = domain->node_id;
    /* set domain tag of subdomain: */
//  strncpy(subdomain->host, domain->host, DART_LOCALITY_HOST_MAX_SIZE);
    int base_tag_len = 0;
    if (domain->level > 0) {
      base_tag_len = sprintf(subdomain->domain_tag, "%s",
                             domain->domain_tag);
    }
    /* append the subdomain tag part to subdomain tag, e.g. ".0.1": */
    sprintf(subdomain->domain_tag + base_tag_len, ".%d", rel_idx);

    switch (domain->scope) {
      case DART_LOCALITY_SCOPE_GLOBAL:
        dart__base__locality__domain__create_global_subdomain(
          host_topology, unit_mapping, domain, subdomain, rel_idx);
        break;
      case DART_LOCALITY_SCOPE_NODE:
        subdomain->num_nodes        = 1;
        dart__base__locality__domain__create_node_subdomain(
          host_topology, unit_mapping, domain, subdomain, rel_idx);
        break;
      case DART_LOCALITY_SCOPE_MODULE:
        subdomain->num_nodes        = 1;
        dart__base__locality__domain__create_module_subdomain(
          host_topology, unit_mapping, domain, subdomain, rel_idx);
        break;
      case DART_LOCALITY_SCOPE_NUMA:
        subdomain->num_nodes        = 1;
//      subdomain->hwinfo.num_numa  = 1;
        dart__base__locality__domain__create_numa_subdomain(
          host_topology, unit_mapping, domain, subdomain, rel_idx);
        break;
      case DART_LOCALITY_SCOPE_PACKAGE:
        subdomain->num_nodes        = 1;
//      subdomain->hwinfo.num_numa  = 1;
        dart__base__locality__domain__create_package_subdomain(
          host_topology, unit_mapping, domain, subdomain, rel_idx);
      case DART_LOCALITY_SCOPE_CACHE:
//      subdomain->hwinfo.num_numa  = 1;
        dart__base__locality__domain__create_cache_subdomain(
          host_topology, unit_mapping, domain, subdomain, rel_idx);
      case DART_LOCALITY_SCOPE_CORE:
//      subdomain->hwinfo.num_numa  = 1;
        subdomain->num_nodes        = 1;
        subdomain->num_units        = 1;
        subdomain->unit_ids         = malloc(sizeof(dart_unit_t));
        subdomain->unit_ids[0]      = domain->unit_ids[rel_idx];
        break;
      default:
        break;
    }
    /* recursively initialize subdomains: */
    DART_ASSERT_RETURNS(
        dart__base__locality__domain__create_subdomains(
          subdomain, host_topology, unit_mapping),
        DART_OK);

    num_domain_units += subdomain->num_units;
  }
  domain->num_units = num_domain_units;

  if (domain->num_domains > 0 &&
      domain->scope > DART_LOCALITY_SCOPE_PACKAGE) {
//  memcpy(&domain->hwinfo.cache_ids,
//         &domain->domains[0].hwinfo.cache_ids,
//         sizeof(domain->hwinfo.cache_ids));
//  memcpy(&domain->hwinfo.cache_sizes,
//         &domain->domains[0].hwinfo.cache_sizes,
//         sizeof(domain->hwinfo.cache_sizes));
  } else {
//  int blank_cache[3] = { -1, -1, -1 };
//  memcpy(&domain->hwinfo.cache_ids,
//         blank_cache, sizeof(blank_cache));
//  memcpy(&domain->hwinfo.cache_sizes,
//         blank_cache, sizeof(blank_cache));
  }

  DART_LOG_DEBUG("dart__base__locality__domain__create_subdomains >");
  return DART_OK;
}

/* ===================================================================== *
 * Private Functions: Implementations                                    *
 * ===================================================================== */

/**
 * Creates a single node subdomain of the global domain.
 */
dart_ret_t dart__base__locality__domain__create_global_subdomain(
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping,
  dart_domain_locality_t         * global_domain,
  dart_domain_locality_t         * subdomain,
  int                              rel_idx)
{
  /* Loop iterates on nodes. Partitioning is trivial, split into one
   * node per sub-domain. */
  dart__unused(global_domain);
  dart__unused(unit_mapping);

  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== SPLIT GLOBAL ==");
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== %d of %d", rel_idx, global_domain->num_domains);
  /* units in node at relative index: */
  const char * node_hostname;
  DART_ASSERT_RETURNS(
    dart__base__host_topology__node(
      host_topology,
      rel_idx,
      &node_hostname),
    DART_OK);
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "host: %s", node_hostname);
//strncpy(subdomain->host, node_hostname, DART_LOCALITY_HOST_MAX_SIZE);

  dart_unit_t * node_unit_ids;
  int           num_node_units;
  DART_ASSERT_RETURNS(
    dart__base__host_topology__node_units(
      host_topology,
      node_hostname,
      &node_unit_ids,
      &num_node_units),
    DART_OK);
  /* relative sub-domain index at global scope is node id: */
//subdomain->node_id              = rel_idx;
  subdomain->relative_index       = rel_idx;
  subdomain->num_nodes            = 1;
  subdomain->num_units            = num_node_units;
//subdomain->hwinfo.shared_mem_kb = subdomain->hwinfo.system_memory * 1024;
  if (subdomain->num_units > 0) {
    subdomain->unit_ids = malloc(subdomain->num_units *
                                 sizeof(dart_unit_t));
  } else {
    subdomain->unit_ids = NULL;
  }
  for (int nu = 0; nu < num_node_units; ++nu) {
    subdomain->unit_ids[nu] = node_unit_ids[nu];
  }
  return DART_OK;
}

/**
 * Creates a single module subdomain of a node domain.
 */
dart_ret_t dart__base__locality__domain__create_node_subdomain(
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping,
  dart_domain_locality_t         * node_domain,
  dart_domain_locality_t         * subdomain,
  int                              rel_idx)
{
  /* Loop splits into processing modules.
   * Usually there is only one module (the host system), otherwise
   * partitioning is heterogenous. */

  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== SPLIT NODE ==");
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== %d of %d", rel_idx, node_domain->num_domains);
  const char * node_hostname;
  const char * module_hostname;
  DART_ASSERT_RETURNS(
    dart__base__host_topology__node(
      host_topology,
      node_domain->relative_index,
      &node_hostname),
    DART_OK);
  DART_ASSERT_RETURNS(
    dart__base__host_topology__node_module(
      host_topology,
//    node_domain->host,
      node_hostname,
      rel_idx,
      &module_hostname),
    DART_OK);
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: host: %s",
                 module_hostname);
  /* set subdomain hostname to module's hostname: */
//  strncpy(subdomain->host, module_hostname,
//          DART_LOCALITY_HOST_MAX_SIZE);
//  strncpy(subdomain->host, module_hostname, DART_LOCALITY_HOST_MAX_SIZE);

  dart_unit_t * module_unit_ids;
  int           num_module_units;
  int           num_module_numa_domains;
  DART_ASSERT_RETURNS(
    dart__base__host_topology__module_units(
      host_topology,
      module_hostname,
      &module_unit_ids,
      &num_module_units,
      &num_module_numa_domains),
    DART_OK);
  subdomain->num_nodes        = 1;
  subdomain->num_units        = num_module_units;
/*
  subdomain->hwinfo.num_numa  = num_module_numa_domains;
*/
  if (subdomain->num_units > 0) {
    subdomain->unit_ids = malloc(subdomain->num_units * sizeof(dart_unit_t));
  } else {
    subdomain->unit_ids = NULL;
  }

  /* Resolve the number of units in the module and distribute number of
   * cores in the module among them: */
//subdomain->hwinfo.num_numa = 1;
  for (int nu = 0; nu < num_module_units; ++nu) {
    subdomain->unit_ids[nu] = module_unit_ids[nu];
    dart_unit_locality_t * module_unit_loc;
    DART_ASSERT_RETURNS(
      dart__base__unit_locality__at(
        unit_mapping, module_unit_ids[nu], &module_unit_loc),
      DART_OK);
    int num_parent_cores     = module_unit_loc->hwinfo.num_cores;
    int num_unit_cores       = num_parent_cores / num_module_units;
    int num_unbalanced_cores = num_parent_cores -
                               (num_unit_cores * num_module_units);
    if (num_unbalanced_cores > 0 && nu < num_unbalanced_cores) {
      /* Unbalanced split of cores to units, e.g. 12 cores / 10 units.
       * First units ordered by unit id get additional core assigned, e.g.
       * unit 0 and 1: */
      num_unit_cores++;
    }
    module_unit_loc->hwinfo.num_cores = num_unit_cores;
    /* as the number of NUMA domains is identical within a module, simply
     * obtain it from hwinfo of first unit in the module: */
//  subdomain->hwinfo.num_numa = module_unit_loc->hwinfo.num_numa;
  }
  return DART_OK;
}

/**
 * Creates a single NUMA node subdomain of a module domain.
 */
dart_ret_t dart__base__locality__domain__create_module_subdomain(
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping,
  dart_domain_locality_t         * module_domain,
  dart_domain_locality_t         * subdomain,
  int                              rel_idx)
{
  /* Loop splits into NUMA nodes. */
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== SPLIT MODULE ==");
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== %d of %d, units:%d",
                 rel_idx,
                 module_domain->num_domains, module_domain->num_units);

  size_t        num_numa_units  = 0;
  /* set subdomain hostname to module's hostname: */
//char *        module_hostname = module_domain->host;
  const char *  node_hostname;
  const char *  module_hostname;
  DART_ASSERT_RETURNS(
    dart__base__host_topology__node(
      host_topology,
      module_domain->parent->relative_index,
      &node_hostname),
    DART_OK);
  DART_ASSERT_RETURNS(
    dart__base__host_topology__node_module(
      host_topology,
      node_hostname,
      module_domain->relative_index,
      &module_hostname),
    DART_OK);
//strncpy(subdomain->host, module_hostname, DART_LOCALITY_HOST_MAX_SIZE);
  int           num_module_units;
  int           num_module_numa_domains;
  dart_unit_t * module_unit_ids;
  DART_ASSERT_RETURNS(
    dart__base__host_topology__module_units(
        host_topology,
        module_hostname,
        &module_unit_ids,
        &num_module_units,
        &num_module_numa_domains),
    DART_OK);
  /* Assign units in the node that have the NUMA domain's NUMA id: */
  /* First pass: Count number of units in NUMA domain: */
  for (int mu = 0; mu < num_module_units; ++mu) {
    dart_unit_t module_unit_id = module_unit_ids[mu];
    /* query the unit's hw- and locality information: */
    dart_unit_locality_t * module_unit_loc;
    dart__base__unit_locality__at(
      unit_mapping, module_unit_id, &module_unit_loc);
    int module_unit_numa_id = module_unit_loc->hwinfo.numa_id;
    /*
     * TODO: assuming that rel_idx corresponds to NUMA id:
     */
    DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                   "unit %d numa id: %d module numa id: %d",
//                 mu, module_unit_numa_id, subdomain->hwinfo.numa_id);
                   mu, module_unit_numa_id, subdomain->relative_index);
    if (module_unit_numa_id == rel_idx) {
      ++num_numa_units;
    }
  }
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "number of units in NUMA domain %d: %ld",
                  rel_idx, num_numa_units);
//subdomain->num_nodes            = 1;
//subdomain->hwinfo.num_numa      = 1;
//subdomain->hwinfo.shared_mem_kb = subdomain->hwinfo.numa_memory * 1024;
  subdomain->num_units            = num_numa_units;
  if (subdomain->num_units > 0) {
    subdomain->unit_ids = malloc(subdomain->num_units *
                                 sizeof(dart_unit_t));
  } else {
    subdomain->unit_ids = NULL;
  }
  /* Second pass: Assign unit ids in NUMA domain: */
  dart_unit_t numa_unit_idx = 0;
  for (int mu = 0; mu < num_module_units; ++mu) {
    dart_unit_t module_unit_id = module_unit_ids[mu];
    /* query the unit's hw- and locality information: */
    dart_unit_locality_t * module_unit_loc;
    dart__base__unit_locality__at(
      unit_mapping, module_unit_id, &module_unit_loc);
    int module_unit_numa_id = module_unit_loc->hwinfo.numa_id;
    /*
     * TODO: assuming that rel_idx corresponds to NUMA id:
     */
    if (module_unit_numa_id == rel_idx) {
      dart_unit_t numa_unit_id = module_unit_id;
      DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                     "NUMA unit %d of %d: unit id %d",
                     numa_unit_idx, subdomain->num_units, numa_unit_id);
      subdomain->unit_ids[numa_unit_idx] = numa_unit_id;
      ++numa_unit_idx;
    }
  }
  return DART_OK;
}

/**
 * Creates a single core node subdomain of a NUMA domain.
 */
dart_ret_t dart__base__locality__domain__create_numa_subdomain(
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping,
  dart_domain_locality_t         * numa_domain,
  dart_domain_locality_t         * subdomain,
  int                              rel_idx)
{
  /* Loop splits into UMA segments within a NUMA domain or module.
   */
  dart__unused(host_topology);
  dart__unused(unit_mapping);

  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== SPLIT NUMA ==");
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== %d of %d, units:%d",
                 rel_idx,
                 numa_domain->num_domains, numa_domain->num_units);

  /*
   * TODO: Assuming that segments are homogenous such that total number
   *       of cores and units is evenly distributed among subdomains.
   */
  int num_domains            = numa_domain->num_domains;
  int num_uma_units          = numa_domain->num_units / num_domains;
//int num_parent_cores       = numa_domain->hwinfo.num_cores;
  int num_parent_cores       = numa_domain->num_cores;
  int num_uma_cores          = num_parent_cores / num_domains;
  int num_unbalanced_cores   = num_parent_cores -
                               (num_uma_cores * num_domains);
  subdomain->num_nodes       = 1;
//subdomain->hwinfo.num_numa = 1;
  subdomain->num_units       = num_uma_units;

  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "num_parent_cores:%d num_uma_units:%d num_domains:%d "
                 "num_unbalanced_cores:%d",
                 num_parent_cores, num_uma_units, num_domains,
                 num_unbalanced_cores);

  if (num_uma_cores < 1) {
    /* Underflow split of cores to UMA nodes, e.g. 10/12.
     * TODO: Clarify if this case occurs and if defaulting to 1 is
     *       sufficient. */
    num_uma_cores = 1;
  } else if (num_unbalanced_cores > 0) {
    /* Unbalanced split of cores to UMA nodes, e.g. 12 units / 10 nodes.
     * First units ordered by unit id get additional core assigned, e.g.
     * unit 0 and 1: */
    if ((int)(rel_idx) < num_unbalanced_cores) {
      num_uma_cores++;
    }
  }
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "=> num_uma_cores:%d", num_uma_cores);

//subdomain->hwinfo.num_cores = num_uma_cores;
  subdomain->num_cores = num_uma_cores;

  if (subdomain->num_units > 0) {
    subdomain->unit_ids = malloc(subdomain->num_units *
                                 sizeof(dart_unit_t));
  } else {
    subdomain->unit_ids = NULL;
  }
  for (int u = 0; u < subdomain->num_units; ++u) {
    int numa_unit_idx      = (rel_idx * num_uma_units) + u;
    dart_unit_t unit_id    = numa_domain->unit_ids[numa_unit_idx];
    subdomain->unit_ids[u] = unit_id;
  }
  return DART_OK;
}

/**
 * Creates a single core node subdomain of a core package domain.
 */
dart_ret_t dart__base__locality__domain__create_package_subdomain(
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping,
  dart_domain_locality_t         * package_domain,
  dart_domain_locality_t         * subdomain,
  int                              rel_idx)
{
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== SPLIT PACKAGE ==");
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== %d of %d, units:%d",
                 rel_idx,
                 package_domain->num_domains, package_domain->num_units);

  dart__unused(host_topology);

  size_t num_domains            = package_domain->num_domains;
  size_t num_cachedom_units     = package_domain->num_units / num_domains;
//size_t num_parent_cores       = package_domain->hwinfo.num_cores;
  size_t num_parent_cores       = package_domain->num_cores;
  size_t num_cachedom_cores     = num_parent_cores / num_domains;
  size_t num_unbalanced_cores   = num_parent_cores -
                                  (num_cachedom_cores * num_domains);
  // Package at cache level 3 so package sub-domain at 2:
  int    cache_level            = 2;
  subdomain->num_nodes          = 1;
//subdomain->hwinfo.num_numa    = 1;
  subdomain->num_units          = num_cachedom_units;
//subdomain->hwinfo.num_cores   = num_cachedom_cores;
  subdomain->num_cores          = num_cachedom_cores;

  if (num_cachedom_cores < 1) {
    /* Underflow split of cores to UMA nodes, e.g. 10/12.
     * TODO: Clarify if this case occurs and if defaulting to 1 is
     *       sufficient. */
    num_cachedom_cores = 1;
  } else if (num_unbalanced_cores > 0) {
    /* Unbalanced split of cores to UMA nodes, e.g. 12 units / 10 nodes.
     * First units ordered by unit id get additional core assigned, e.g.
     * unit 0 and 1: */
    if ((size_t)(rel_idx) < num_unbalanced_cores) {
      num_cachedom_cores++;
    }
  }

  if (subdomain->num_units > 0) {
    subdomain->unit_ids = malloc(subdomain->num_units *
                                 sizeof(dart_unit_t));
  } else {
    subdomain->unit_ids = NULL;
  }

  /* Determine number of caches: */

  int * cache_ids = malloc(sizeof(int) * subdomain->num_units);
  for (int u = 0; u < subdomain->num_units; ++u) {
    int cachedom_unit_idx  = (rel_idx * num_cachedom_units) + u;
    dart_unit_t unit_id    = package_domain->unit_ids[cachedom_unit_idx];
    subdomain->unit_ids[u] = unit_id;
    /* set host and domain tag of unit in unit locality map: */
    dart_unit_locality_t * unit_loc;
    DART_ASSERT_RETURNS(
      dart__base__unit_locality__at(unit_mapping, unit_id, &unit_loc),
      DART_OK);
    cache_ids[u] = unit_loc->hwinfo.cache_ids[cache_level];
  }

  int num_separate_caches =
        dart__base__intsunique(cache_ids, subdomain->num_units);

//subdomain->hwinfo.shared_mem_kb = -2;
  subdomain->shared_mem_kb        = -2;
  subdomain->num_domains          = num_separate_caches;

  if (cache_ids[0] < 0) {
    subdomain->scope            = DART_LOCALITY_SCOPE_CORE;
    subdomain->num_domains      = 1;
    subdomain->num_units        = 1;

    int cachedom_unit_idx  = (rel_idx * num_cachedom_units);
    dart_unit_t unit_id    = package_domain->unit_ids[cachedom_unit_idx];
    subdomain->unit_ids[0] = unit_id;

    /* set host and domain tag of unit in unit locality map: */
    dart_unit_locality_t * unit_loc;
    DART_ASSERT_RETURNS(
      dart__base__unit_locality__at(unit_mapping, unit_id, &unit_loc),
      DART_OK);
    DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                   "setting "
                   "unit %d domain_tag: %s",
                   unit_id, subdomain->domain_tag);
//  strncpy(unit_loc->domain_tag, subdomain->domain_tag,
//          DART_LOCALITY_DOMAIN_TAG_MAX_SIZE);
    strncpy(unit_loc->domain.domain_tag, subdomain->domain_tag,
            DART_LOCALITY_DOMAIN_TAG_MAX_SIZE);
//  memcpy(&subdomain->hwinfo.cache_ids, &unit_loc->hwinfo.cache_ids,
//         sizeof(unit_loc->hwinfo.cache_ids));
//  memcpy(&subdomain->hwinfo.cache_sizes, &unit_loc->hwinfo.cache_sizes,
//         sizeof(unit_loc->hwinfo.cache_sizes));
  }

  free(cache_ids);

  return DART_OK;
}

/**
 * Creates a single core node subdomain of a cache domain.
 */
dart_ret_t dart__base__locality__domain__create_cache_subdomain(
  dart_host_topology_t           * host_topology,
  dart_unit_mapping_t            * unit_mapping,
  dart_domain_locality_t         * cache_domain,
  dart_domain_locality_t         * subdomain,
  int                              rel_idx)
{
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== SPLIT CACHE ==");
  DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                 "== %d of %d, units:%d",
                 rel_idx,
                 cache_domain->num_domains, cache_domain->num_units);

  dart__unused(host_topology);

  size_t num_domains            = cache_domain->num_domains;
  size_t num_cachedom_units     = cache_domain->num_units / num_domains;
//size_t num_parent_cores       = cache_domain->hwinfo.num_cores;
  size_t num_parent_cores       = cache_domain->num_cores;
  size_t num_cachedom_cores     = num_parent_cores / num_domains;
  size_t num_unbalanced_cores   = num_parent_cores -
                                  (num_cachedom_cores * num_domains);
  // Package at cache level 3, package cache subdomain (= cache_domain)
  // at 2, so cache sub-domain at 1:
  int    cache_level            = 2;
  subdomain->num_nodes          = 1;
//subdomain->hwinfo.num_numa    = 1;
  subdomain->num_units          = num_cachedom_units;
//subdomain->hwinfo.num_cores   = num_cachedom_cores;
  subdomain->num_cores          = num_cachedom_cores;

  if (num_cachedom_cores < 1) {
    /* Underflow split of cores to UMA nodes, e.g. 10/12.
     * TODO: Clarify if this case occurs and if defaulting to 1 is
     *       sufficient. */
    num_cachedom_cores = 1;
  } else if (num_unbalanced_cores > 0) {
    /* Unbalanced split of cores to UMA nodes, e.g. 12 units / 10 nodes.
     * First units ordered by unit id get additional core assigned, e.g.
     * unit 0 and 1: */
    if ((size_t)(rel_idx) < num_unbalanced_cores) {
      num_cachedom_cores++;
    }
  }

  if (cache_domain->scope == DART_LOCALITY_SCOPE_CACHE) {
    cache_level--;
  }
  if (cache_domain->parent->scope == DART_LOCALITY_SCOPE_CACHE) {
    cache_level--;
  }

  if (subdomain->num_units > 0) {
    subdomain->unit_ids = malloc(subdomain->num_units *
                                 sizeof(dart_unit_t));
  } else {
    subdomain->unit_ids = NULL;
  }
  for (int u = 0; u < subdomain->num_units; ++u) {
    int unit_idx           = (rel_idx * num_cachedom_units) + u;
    dart_unit_t unit_id    = cache_domain->unit_ids[unit_idx];
    subdomain->unit_ids[u] = unit_id;
    subdomain->num_domains = subdomain->num_units;

    if (subdomain->scope == DART_LOCALITY_SCOPE_CORE) { //cache_level == 0) {
      /* set host and domain tag of unit in unit locality map: */
      dart_unit_locality_t * unit_loc;
      DART_ASSERT_RETURNS(
        dart__base__unit_locality__at(unit_mapping, unit_id, &unit_loc),
        DART_OK);
      DART_LOG_TRACE("dart__base__locality__domain__create_subdomains: "
                     "setting "
                     "unit %d domain_tag: %s",
                     unit_id, subdomain->domain_tag);
      strncpy(unit_loc->domain.domain_tag, subdomain->domain_tag,
              DART_LOCALITY_DOMAIN_TAG_MAX_SIZE);
//    memcpy(&subdomain->hwinfo.cache_ids, &unit_loc->hwinfo.cache_ids,
//           sizeof(unit_loc->hwinfo.cache_ids));
//    memcpy(&subdomain->hwinfo.cache_sizes, &unit_loc->hwinfo.cache_sizes,
//           sizeof(unit_loc->hwinfo.cache_sizes));
    }
  }
//subdomain->hwinfo.shared_mem_kb
//  = subdomain->hwinfo.cache_sizes[cache_level] / 1024;
  return DART_OK;
}
