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
  return std::find_if(elems.data.begin(), elems.data.end(),
                      [&elem](const std::pair<T, uid_t> &pair) { return pair.first == elem; }) != elems.data.end();
  // throw NotImplementedException("ORSet<T>::Contains is not implemented");
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  if (std::find_if(tomb.data.begin(), tomb.data.end(),
                   [&elem](const std::pair<T, uid_t> &pair) { return pair.first == elem; }) == tomb.data.end()) {
    elems.data.push_back(std::make_pair(elem, uid));
  }
  // throw NotImplementedException("ORSet<T>::Add is not implemented");
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  auto it = std::find_if(elems.data.begin(), elems.data.end(),
                         [&elem](const std::pair<T, uid_t> &pair) { return pair.first == elem; });
  if (it != elems.data.end()) {
    tomb.data.push_back(*it);
    elems.data.erase(it);
  }

  // throw NotImplementedException("ORSet<T>::Remove is not implemented");
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  // Find elements in A but in tomb of B
  for (const auto &pair : other.elems.data) {
    if (std::find_if(tomb.data.begin(), tomb.data.end(), [&pair](const std::pair<T, uid_t> &tombPair) {
          return tombPair.first == pair.first;
        }) == tomb.data.end()) {
      elems.data.push_back(pair);
    }
  }
  for (const auto &pair : other.tomb.data) {
    if (std::find_if(elems.data.begin(), elems.data.end(), [&pair](const std::pair<T, uid_t> &elemPair) {
          return elemPair.first == pair.first;
        }) == elems.data.end()) {
      tomb.data.push_back(pair);
    }
  }
  for (auto it = elems.data.begin(); it != elems.data.end();) {
    if (std::find_if(other.tomb.data.begin(), other.tomb.data.end(), [&it](const std::pair<T, uid_t> &tombPair) {
          return tombPair.first == it->first;
        }) != other.tomb.data.end()) {
      it = elems.data.erase(it);
    } else {
      ++it;
    }
  }
  // throw NotImplementedException("ORSet<T>::Merge is not implemented");
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T> result;
  std::transform(elems.data.begin(), elems.data.end(), std::back_inserter(result),
                 [](const std::pair<T, uid_t> &pair) { return pair.first; });
  return result;
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
