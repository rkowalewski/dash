#ifndef DASH__VIEW__VIEW_BLOCKS_MOD_H__INCLUDED
#define DASH__VIEW__VIEW_BLOCKS_MOD_H__INCLUDED

#include <dash/Types.h>
#include <dash/Range.h>
#include <dash/Iterator.h>

#include <dash/view/IndexSet.h>
#include <dash/view/ViewTraits.h>
#include <dash/view/ViewMod.h>

#include <dash/view/Local.h>
#include <dash/view/Global.h>
#include <dash/view/Origin.h>
#include <dash/view/Domain.h>
#include <dash/view/Apply.h>
#include <dash/view/Chunked.h>
#include <dash/view/Sub.h>
// #include <dash/view/SetIntersect.h>


namespace dash {

#ifndef DOXYGEN

// ------------------------------------------------------------------------
// Forward-declarations
// ------------------------------------------------------------------------
//
template <
  class DomainType = ViewOrigin<1> >
class ViewBlocksMod;

// ------------------------------------------------------------------------
// ViewBlockMod
// ------------------------------------------------------------------------

template <
  class DomainType >
struct view_traits<ViewBlockMod<DomainType> > {
  typedef DomainType                                           domain_type;
  typedef typename view_traits<domain_type>::origin_type       origin_type;
  typedef typename view_traits<domain_type>::pattern_type     pattern_type;
  typedef ViewBlockMod<DomainType>                              image_type;
  typedef ViewBlockMod<DomainType>                              local_type;
  typedef ViewBlockMod<DomainType>                             global_type;

  typedef typename DomainType::index_type                       index_type;
  typedef dash::IndexSetSub<ViewBlockMod<DomainType>>       index_set_type;

  typedef std::integral_constant<bool, false>                is_projection;
  typedef std::integral_constant<bool, true>                 is_view;
  typedef std::integral_constant<bool, false>                is_origin;
  typedef std::integral_constant<bool,
    view_traits<domain_type>::is_local::value >              is_local;

  typedef std::integral_constant<dim_t, 1>                            rank;
};


template <
  class DomainType >
class ViewBlockMod
// Actually just an adapter for block_idx -> sub(begin_idx, end_idx),
// should sublass
//
//   public ViewSubMod<DomainType, 0>
//
: public ViewModBase <
           ViewBlockMod<DomainType>,
           DomainType >
{
 public:
  typedef DomainType                                           domain_type;
  typedef typename view_traits<DomainType>::index_type          index_type;
 private:
  typedef ViewBlockMod<DomainType>                                  self_t;
  typedef ViewModBase< ViewBlockMod<DomainType>, DomainType >
                                                                    base_t;
 public:
//typedef dash::IndexSetSub< ViewBlockMod<DomainType> >     index_set_type;
  typedef dash::IndexSetSub< DomainType >                   index_set_type;
  typedef ViewLocalMod<self_t>                                  local_type;
  typedef self_t                                               global_type;

  typedef std::integral_constant<bool, false>                     is_local;

 private:
  index_type     _block_idx;
  index_type     _first_idx;
  index_type     _final_idx;
  index_set_type _index_set;

 public:
  constexpr ViewBlockMod()               = delete;
  constexpr ViewBlockMod(self_t &&)      = default;
  constexpr ViewBlockMod(const self_t &) = default;
  ~ViewBlockMod()                        = default;
  self_t & operator=(self_t &&)          = delete;
  self_t & operator=(const self_t &)     = delete;

  constexpr ViewBlockMod(
    const DomainType & domain,
    index_type         block_idx)
  : base_t(domain)
  , _block_idx(block_idx)
  , _index_set(domain, // *this,
               block_first_gidx(domain, block_idx),
               block_final_gidx(domain, block_idx))
//, _first_idx(block_first_gidx(domain, block_idx))
//, _final_idx(block_final_gidx(domain, block_idx))
//, _index_set(*this, _first_idx, _final_idx)
  { }

  constexpr auto begin() const
  -> decltype(dash::begin(
                std::declval<
                  typename std::add_lvalue_reference<domain_type>::type
                >() )) {
    return dash::begin(dash::domain(*this)) +
              dash::index(*this)[0];
           // _index_set.first();
  }

  constexpr auto end() const
  -> decltype(dash::begin(
                std::declval<
                  typename std::add_lvalue_reference<domain_type>::type
                >() )) {
    return dash::begin(dash::domain(*this)) +
           //  (dash::index(*this).last()) + 1;
               _index_set.last() + 1;
  }

  constexpr auto operator[](int offset) const
  -> decltype(*(dash::begin(
                  std::declval<
                    typename std::add_lvalue_reference<domain_type>::type
                  >() ))) {
    return dash::begin(*this)[offset];
  }

  constexpr const index_set_type & index_set() const {
    return _index_set;
  }

  constexpr local_type local() const {
    return local_type(*this);
  }

 private:
  /// Block index of first element in view
  ///
  constexpr index_type block_first_gidx(
      const DomainType & vdomain,
      index_type         block_idx) const {
    //
    // TODO: If domain is local, use pattern().local_block(block_idx)
    //
    return std::max(
             ( // block viewspec (extents, offsets)
               dash::index(vdomain)
                 .pattern().block(block_idx).offsets()[0]
             ),
             dash::index(vdomain).first()
           )
           - dash::index(vdomain).first();
  }

  /// Index past block index of last element in view:
  ///
  constexpr index_type block_final_gidx(
      const DomainType & vdomain,
      index_type         block_idx) const {
    //
    // TODO: If domain is local, use pattern().local_block(block_idx)
    //
    return std::min<index_type>(
             dash::index(vdomain).last() + 1,
             ( // block viewspec (extents, offsets)
               dash::index(vdomain)
                 .pattern().block(block_idx).offsets()[0]
             + dash::index(vdomain)
                 .pattern().block(block_idx).extents()[0]
             )
           )
           - dash::index(vdomain).first();
  }
};

// ------------------------------------------------------------------------
// ViewBlocksMod
// ------------------------------------------------------------------------

template <class ViewType>
constexpr ViewBlocksMod<ViewType>
blocks(const ViewType & domain) {
  return ViewBlocksMod<ViewType>(domain);
}

template <
  class DomainType >
struct view_traits<ViewBlocksMod<DomainType> > {
  typedef DomainType                                           domain_type;
  typedef typename view_traits<domain_type>::origin_type       origin_type;
  typedef typename view_traits<domain_type>::pattern_type     pattern_type;
  typedef ViewBlocksMod<DomainType>                             image_type;
  typedef typename domain_type::local_type                      local_type;
  typedef ViewBlocksMod<DomainType>                            global_type;

  typedef typename DomainType::index_type                       index_type;
  typedef dash::IndexSetBlocks<ViewBlocksMod<DomainType>>   index_set_type;

  typedef std::integral_constant<bool, false>                is_projection;
  typedef std::integral_constant<bool, true>                 is_view;
  typedef std::integral_constant<bool, false>                is_origin;
  typedef std::integral_constant<bool, false>                is_local;
};

template <
  class DomainType >
class ViewBlocksMod
: public ViewModBase< ViewBlocksMod<DomainType>, DomainType > {
 public:
  typedef DomainType                                           domain_type;
  typedef typename view_traits<DomainType>::origin_type        origin_type;
  typedef typename view_traits<DomainType>::index_type          index_type;
 private:
  typedef ViewBlocksMod<DomainType>                                 self_t;
  typedef ViewModBase<ViewBlocksMod<DomainType>, DomainType>        base_t;
  typedef ViewBlockMod<DomainType>                              block_type;
 public:
  typedef dash::IndexSetBlocks<ViewBlocksMod<DomainType>>   index_set_type;
  typedef self_t                                               global_type;
  typedef typename domain_type::local_type                      local_type;

  typedef std::integral_constant<bool, false>                     is_local;

 private:
  index_set_type  _index_set;

 public:
  class block_iterator
  : public internal::IndexIteratorBase<
             block_iterator,
             block_type,
             index_type,
             std::nullptr_t,
             block_type > {
   private:
    typedef internal::IndexIteratorBase<
              block_iterator,
              block_type,     // value type
              index_type,     // difference type
              std::nullptr_t, // pointer type
              block_type >    // reference type
      iterator_base_t;
   private:
    const ViewBlocksMod<DomainType> * const _blocks_view;
   public:
    constexpr block_iterator()                         = delete;
    constexpr block_iterator(block_iterator &&)        = default;
    constexpr block_iterator(const block_iterator &)   = default;
    ~block_iterator()                                  = default;
    block_iterator & operator=(block_iterator &&)      = delete;
    block_iterator & operator=(const block_iterator &) = delete;

    constexpr block_iterator(
      const block_iterator            & other,
      index_type                        position)
    : iterator_base_t(position)
    , _blocks_view(other._blocks_view)
    { }

    constexpr block_iterator(
      const ViewBlocksMod<DomainType> & blocks_view,
      index_type                        position)
    : iterator_base_t(position)
    , _blocks_view(&blocks_view)
    { }

    constexpr block_type dereference(index_type idx) const {
      // Dereferencing block iterator returns block at block index
      // with iterator position.
      // Note that block index is relative to the domain and is
      // translated to global block index in IndexSetBlocks.
      // return dash::block(idx, (dash::domain(*_blocks_view)));
      return ViewBlockMod<DomainType>(dash::domain(*_blocks_view), idx);
    }
  };

 public:
  constexpr ViewBlocksMod()               = delete;
  constexpr ViewBlocksMod(self_t &&)      = default;
  constexpr ViewBlocksMod(const self_t &) = default;
  ~ViewBlocksMod()                        = default;
  self_t & operator=(self_t &&)           = delete;
  self_t & operator=(const self_t &)      = delete;

  constexpr explicit ViewBlocksMod(
    const DomainType & domain)
  : base_t(domain)
  , _index_set(*this)
  { }

  constexpr block_iterator begin() const {
    return block_iterator(*this, this->index_set().first());
  }

  constexpr block_iterator end() const {
    return block_iterator(*this, this->index_set().last() + 1);
  }

  constexpr block_type operator[](int offset) const {
    return *(dash::begin(*this) + offset);
  }

  constexpr auto local() const
//  -> decltype(dash::local(dash::domain(*this))) {
    -> decltype(dash::local(
                  std::declval<
                    typename std::add_lvalue_reference<domain_type>::type
                  >() )) {
    return dash::local(dash::domain(*this));
  }

  inline auto local()
//  -> decltype(dash::local(dash::domain(*this))) {
    -> decltype(dash::local(
                  std::declval<
                    typename std::add_lvalue_reference<domain_type>::type
                  >() )) {
    return dash::local(dash::domain(*this));
  }

  constexpr const global_type & global() const {
    return dash::global(dash::domain(*this));
  }

  inline global_type & global() {
    return dash::global(dash::domain(*this));
  }

  constexpr const index_set_type & index_set() const {
    return _index_set;
  }
};

#endif // DOXYGEN

} // namespace dash

#endif // DASH__VIEW__VIEW_BLOCKS_MOD_H__INCLUDED
