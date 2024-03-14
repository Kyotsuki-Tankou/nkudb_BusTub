#pragma once

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

namespace bustub {

/** @brief Unique ID type. */
using uid_t = int64_t;

/** @brief The observed remove set datatype. */
template <typename S, typename T>
class KeyValueVector {
 public:
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

  void pAll() {
    std::cout << "elems:";
    for (const auto &elem : elems.data) {
      std::cout << '(' << elem.first << ',' << elem.second << ") ";
    }
    std::cout << "\ntomb:";
    for (const auto &elem : tomb.data) {
      std::cout << '(' << elem.first << ',' << elem.second << ") ";
    }
    std::cout << '\n';
  }

 private:
  // TODO(student): Add your private memeber variables to represent ORSet.
  KeyValueVector<T, uid_t> elems;
  KeyValueVector<T, uid_t> tomb;
};

}  // namespace bustub
