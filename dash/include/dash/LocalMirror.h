#ifndef DASH__LOCAL_MIRROR_H__INCLUDED
#define DASH__LOCAL_MIRROR_H__INCLUDED

#include <dash/Types.h>

namespace dash {

template <class T, class MemorySpace>
class LocalMirror {
  using pointer_traits =
      std::pointer_traits<typename MemorySpace::local_void_pointer>;

public:
  /// public type-defs
  using value_type = T;
  using size_type = dash::default_size_t;

  using pointer = typename pointer_traits::template rebind<value_type>;
  using const_pointer =
      typename pointer_traits::template rebind<const value_type>;

public:
  LocalMirror()                    = default;
  LocalMirror(const LocalMirror &) = delete;
  LocalMirror(LocalMirror &&)      = default;
  LocalMirror & operator=(LocalMirror const &) = default;
  LocalMirror &&operator=(LocalMirror &&) = default;

  //Construct Local mirror by two global input iterators
  template <class GlobInputIt>
  LocalMirror(GlobInputIt begin, GlobInputIt end);


  template <class GlobInputIt>
  void mirror(GlobInputIt begin, GlobInputIt end);

  template <class GlobInputIt>
  void mirror_async(GlobInputIt begin, GlobInputIt end);

  void reserve(size_type n);

  pointer begin();
  pointer end();


};
}  // namespace dash
#endif
