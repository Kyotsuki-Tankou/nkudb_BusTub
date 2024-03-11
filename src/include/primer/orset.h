#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace bustub {

/** @brief Unique ID type. */
using uid_t = int64_t;

/** @brief The observed remove set datatype. */
template <typename S, typename T>
class KeyValueVector {
 public:
  void put(const S &key, const T &value) {
    auto it = std::find_if(data.begin(), data.end(), [&key](const auto &pair) { return pair.first == key; });
    if (it != data.end()) {
      it->second = value;
    } else {
      data.push_back(std::make_pair(key, value));
    }
  }
  void remove(const S &key) {
    auto it = std::find_if(data.begin(), data.end(), [&key](const auto &pair) { return pair.first == key; });
    if (it != data.end()) {
      data.erase(it, it);
    }
  }
  void update(const S &key, const T &value) {
    auto it = std::find_if(data.begin(), data.end(), [&key](const auto &pair) { return pair.first == key; });
    if (it != data.end()) {
      it->second = value;
    }
  }
  int get(const S &key) const {
    auto it = std::find_if(data.begin(), data.end(), [&key](const auto &pair) { return pair.first == key; });
    if (it != data.end()) {
      return std::distance(data.begin(), it);
    } else {
      return -1;
    }
  }
  std::vector<std::pair<S, T>> data;
};
template <typename T>
class ORSet {
 public:
  ORSet() = default;

  /**
   * @brief Checks if an element is in the set.
   *
   * @param elem the element to check
   * @return true if the element is in the set, and false otherwise.
   */
  auto Contains(const T &elem) const -> bool;

  /**
   * @brief Adds an element to the set.
   *
   * @param elem the element to add
   * @param uid unique token associated with the add operation.
   */
  void Add(const T &elem, uid_t uid);

  /**
   * @brief Removes an element from the set if it exists.
   *
   * @param elem the element to remove.
   */
  void Remove(const T &elem);

  /**
   * @brief Merge changes from another ORSet.
   *
   * @param other another ORSet
   */
  void Merge(const ORSet<T> &other);

  /**
   * @brief Gets all the elements in the set.
   *
   * @return all the elements in the set.
   */
  auto Elements() const -> std::vector<T>;

  /**
   * @brief Gets a string representation of the set.
   *
   * @return a string representation of the set.
   */
  auto ToString() const -> std::string;

 private:
  // TODO(student): Add your private memeber variables to represent ORSet.
  KeyValueVector<T, uid_t> elems;
  KeyValueVector<T, uid_t> tomb;
};

}  // namespace bustub
