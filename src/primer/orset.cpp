#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  return elems.get(elem)!=-1;
  // throw NotImplementedException("ORSet<T>::Contains is not implemented");
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  if(elems.get(elem)==-1&&tomb.get(elem)==-1)
  {
    elems.put(elem,uid);
  }
  // throw NotImplementedException("ORSet<T>::Add is not implemented");
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  int res=elems.get(elem);
  if(res!=-1)
  {
    const auto &pair=elems.data[res];
    elems.remove(pair.first);
    tomb.put(pair.first,pair.second);
  }
  //throw NotImplementedException("ORSet<T>::Remove is not implemented");
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  //Find elements in A but in tomb of B
  
  //Find elements in B and not in tomb of A
  
  //Merge tombs
  for(const auto &pair:other.tomb.data)
  {
    tomb.put(pair.first,pair.second);
  }
  //throw NotImplementedException("ORSet<T>::Merge is not implemented");
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T>res;
  for(const auto &pair:elems.data)
  {
    if(tomb.get(pair.first)==-1)
    {
      res.push_back(pair.first);
    }
  }
  return res;
  // throw NotImplementedException("ORSet<T>::Elements is not implemented");
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
