/* 
 * dash-lib/HView.h
 *
 * author(s): Karl Fuerlinger, LMU Munich 
 */
/* @DASH_HEADER@ */

#ifndef HVIEW_H_INCLUDED
#define HVIEW_H_INCLUDED

#include <iostream>
#include <dash/Team.h>
#include <dash/Pattern.h>

namespace dash {

template<class ContainerType, int LEVEL> 
class HIter : public ContainerType::iterator {
private:
  Pattern<1> & m_pattern;
  Team&        m_subteam;

public:
  HIter<ContainerType,LEVEL>& advance() {
    auto idx = ContainerType::iterator::m_idx;
    for(; idx < m_pattern.capacity(); idx++) {
      auto unit = m_pattern.index_to_unit(idx);
      if (m_subteam.isMember(unit)) {
        break;
      }
    }
    ContainerType::iterator::m_idx = idx;
    return *this;
  }

public:
  HIter(
    typename ContainerType::iterator it, 
    Pattern<1> & pattern,
    Team & subteam)
  : ContainerType::iterator(it), 
    m_pattern(pattern),
    m_subteam(subteam) {
  }

  void print() {
    std::cout << ContainerType::iterator::m_idx << std::endl;
  }

  HIter<ContainerType,LEVEL>& operator++() {
    ContainerType::iterator::m_idx++;
    return advance();
  }
};

template<class ContainerType, int LEVEL>
class HView {
public:
  typedef typename ContainerType::iterator    iterator;
  typedef typename ContainerType::value_type  value_type;
  
private:
  ContainerType & m_container;
  Team&           m_subteam;
  Pattern<1> &    m_pat;

  HIter<ContainerType,LEVEL> m_begin;
  HIter<ContainerType,LEVEL> m_end;

  HIter<ContainerType,LEVEL> find_begin() {
    HIter<ContainerType, LEVEL> it = {m_container.begin(),m_pat,m_subteam};
    it.advance();
    return it;
  }

  HIter<ContainerType, LEVEL> find_end() {
    return { m_container.end(), m_pat,m_subteam };
  }
  
public:
  HView(ContainerType& cont) 
  : m_container(cont), 
    m_subteam(cont.team().sub(LEVEL)),
    m_pat(cont.pattern()),
    m_begin(find_begin()),
    m_end(find_end()) {
  }
  
  void print() {
    std::cout << "This team has size " << m_subteam.size() << std::endl;
  }
  
  HIter<ContainerType, LEVEL> begin() { 
    return m_begin;
  }
  
  HIter<ContainerType, LEVEL> end() { 
    return m_end;
  }
};

template<class ContainerType>
class HView<ContainerType, -1> {
public:
  typedef typename ContainerType::iterator   iterator;
  typedef typename ContainerType::value_type value_type;

private:
  Team &          m_subteam;
  ContainerType & m_container;
  Pattern<1> &    m_pat;

public:
  HView(ContainerType& cont) 
  : m_container(cont), 
    m_subteam(cont.team()),
    m_pat(cont.pattern()) {
  };
  
  value_type* begin() { 
    return m_container.lbegin();
  }
  
  value_type* end() { 
    return m_container.lend();
  }
};

} // namespace dash

#endif /* HVIEW_H_INCLUDED */