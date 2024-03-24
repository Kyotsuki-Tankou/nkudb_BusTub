#include "primer/orset.h"
#include <algorithm>
#include <iostream>
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
  auto it = std::find_if(elems.data.begin(), elems.data.end(), [&elem, &uid](const std::pair<T, uid_t> &pair) {
    return pair.first == elem && pair.second == uid;
  });
  if (it == elems.data.end()) {
    if (std::find_if(tomb.data.begin(), tomb.data.end(), [&elem, uid](const std::pair<T, uid_t> &pair) {
          return pair.first == elem && pair.second == uid;
        }) == tomb.data.end()) {
      elems.data.push_back(std::make_pair(elem, uid));
    }
  }
  // std::cout << "Add" << '\n';
  // pAll();
  // throw NotImplementedException("ORSet<T>::Add is not implemented");
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  std::vector<uid_t> uids_to_remove;
  for (auto it = elems.data.begin(); it != elems.data.end();) {
    if (it->first == elem) {
      uids_to_remove.push_back(it->second);
      it = elems.data.erase(it);
    } else {
      ++it;
    }
  }
  for (uid_t uid : uids_to_remove) {
    tomb.data.push_back(std::make_pair(elem, uid));
  }
  // std::cout << "Remove" << '\n';
  // pAll();
  // throw NotImplementedException("ORSet<T>::Remove is not implemented");
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  for (const auto &pair : other.elems.data) {
    Add(pair.first, pair.second);
  }
  for (const auto &pair : other.tomb.data) {
    auto it = std::find_if(elems.data.begin(), elems.data.end(), [&pair](const std::pair<T, uid_t> &elem_pair) {
      return pair.first == elem_pair.first && pair.second == elem_pair.second;
    });
    if (it != elems.data.end()) {
      tomb.data.push_back(*it);
      elems.data.erase(it);
    } else {
      auto tomb_it = std::find_if(tomb.data.begin(), tomb.data.end(), [&pair](const std::pair<T, uid_t> &tomb_pair) {
        return pair.first == tomb_pair.first && pair.second == tomb_pair.second;
      });
      if (tomb_it == tomb.data.end()) {
        tomb.data.push_back(pair);
      }
    }
  }
  // std::cout << "Merge" << '\n';
  // pAll();
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
