//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT((uint64_t)max_size <= LEAF_PAGE_SIZE, "OUT OF LEAF_PAGE_SIZE\n");
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  array_[index].first = key;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) const -> MappingType {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  return array_[index];
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &comparator, Transaction *txn) const -> bool {
  int size = GetSize();
  for (int id = 0; id < size; id++) {
    if (id == size - 1 || comparator(key, KeyAt(id + 1)) < 0) {
      if (comparator(key, KeyAt(id)) == 0) {
        result->push_back(ValueAt(id));
        return true;
      }
      break;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                        Transaction *txn) -> bool {
  int size = GetSize();
  for (int id = 0; id < size; id++) {
    if (comparator(key, KeyAt(id)) == 0) {
      return false;
    }
  }
  for (int id = size; id >= 0; id--) {
    if (id == 0 || comparator(key, KeyAt(id - 1)) > 0) {
      array_[id] = {key, value};
      break;
    }
    array_[id] = array_[id - 1];
  }
  SetSize(size + 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndexByKey(const KeyType &key, const KeyComparator &comparator,
                                               Transaction *txn) const -> int {
  int size = GetSize();
  for (int id = 0; id < size; id++) {
    if (comparator(key, KeyAt(id)) == 0) {
      return id;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Split(BPlusTreeLeafPage *that, Transaction *txn) -> KeyType {
  int size = GetSize();
  // 0 1 2 3 4     3  (5+1)>>1
  // 0 1 2 3 4 5   3  (6+1)>>1
  // 0 1 2 3 4 5 6 4  (7+1)>>1
  int offset = (size + 1) >> 1;
  int id = 0;
  for (; id + offset < size; id++) {
    that->array_[id] = array_[id + offset];
  }
  SetSize(offset);
  that->SetSize(id);
  return that->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator, Transaction *txn) -> bool {
  int size = GetSize();
  bool flag = false;

  for (int id = 0; id < size; id++) {
    if (flag) {
      array_[id - 1] = array_[id];
      continue;
    }
    if (comparator(key, KeyAt(id)) == 0) {
      flag = true;
    }
  }

  if (!flag) {
    return false;
  }
  SetSize(size - 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Merge(BPlusTreeLeafPage *right_page, KeyType &l_key, KeyType &r_key, bool &remove,
                                       const KeyComparator &comparator, Transaction *txn) {
  // first calc size
  int left_size = GetSize();
  int right_size = right_page->GetSize();
  // only borrow one record
  if (left_size > GetMinSize()) {
    int index = left_size - 1;
    KeyType key = KeyAt(index);
    ValueType value = ValueAt(index);

    Remove(key, comparator);
    right_page->Insert(key, value, comparator);

    r_key = right_page->KeyAt(0);
    l_key = KeyAt(0);

    remove = false;
    return;
  }
  if (right_size > right_page->GetMinSize()) {
    int index = 0;
    KeyType key = right_page->KeyAt(index);
    ValueType value = right_page->ValueAt(index);

    right_page->Remove(key, comparator);
    Insert(key, value, comparator);

    r_key = right_page->KeyAt(0);
    l_key = KeyAt(0);

    remove = false;
    return;
  }
  // merge to one
  for (int id = 0; id < right_size; id++) {
    KeyType key = right_page->KeyAt(id);
    ValueType value = right_page->ValueAt(id);
    Insert(key, value, comparator);
    //    right_page->Remove(key,comparator);
  }
  right_page->SetSize(0);
  l_key = KeyAt(0);
  // left size already increase in Insert function
  remove = true;
  SetNextPageId(right_page->GetNextPageId());
  // delete right page
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
