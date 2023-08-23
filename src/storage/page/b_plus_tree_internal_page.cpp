//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  BUSTUB_ASSERT((uint64_t)max_size <= INTERNAL_PAGE_SIZE, "OUT OF INTERNAL_PAGE_SIZE\n");
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index < GetMaxSize(), "OUT OF MAXSIZE\n");
  BUSTUB_ASSERT(index < GetSize(), "OUT OF SIZE\n");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage *that, Transaction *txn) -> KeyType {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                                            Transaction *txn) {
  int size = GetSize();
  // attention that Key(0) is null
  for (int id = size; id >= 0; id--) {
    if (id == 0 || comparator(key, KeyAt(id - 1)) > 0) {
      array_[id] = {key, value};
      //      if (value == 0) {
      //      }
      break;
    }
    array_[id] = array_[id - 1];
  }
  SetSize(size + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index, Transaction *txn) {
  int size = GetSize();
  for (int id = 0, flag = 0; id < size; id++) {
    if (flag != 0) {
      array_[id - 1] = array_[id];
      continue;
    }
    if (index == id) {
      flag = 1;
    }
  }
  SetSize(size - 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage *right_page, KeyType &l_key, KeyType &r_key,
                                           bool &remove, const KeyComparator &comparator, Transaction *txn) {
  // attention that record[0] doesn't have key!

  // first calc size
  int left_size = GetSize();
  int right_size = right_page->GetSize();
  // only borrow one record
  if (left_size > GetMinSize()) {
    int index = left_size - 1;
    KeyType key = KeyAt(index);
    page_id_t value = ValueAt(index);

    Remove(index);
    right_page->Insert(key, value, comparator);

    r_key = right_page->KeyAt(0);
    l_key = KeyAt(0);

    remove = false;
    return;
  }

  if (right_size > right_page->GetMinSize()) {
    int index = 0;
    KeyType key = right_page->KeyAt(index);
    page_id_t value = right_page->ValueAt(index);

    right_page->Remove(index);
    Insert(key, value, comparator);

    r_key = right_page->KeyAt(0);
    l_key = KeyAt(0);

    remove = false;
    return;
  }
  // merge to one, only from right to left ,to make sure that the iterator for leaf always be 1;
  for (int id = 0; id < right_size; id++) {
    KeyType key = right_page->KeyAt(id);
    page_id_t value = right_page->ValueAt(id);
    Insert(key, value, comparator);
    //    right_page->Remove(id);
  }

  right_page->SetSize(0);
  l_key = KeyAt(0);
  // left size already increase in Insert function
  remove = true;

  // delete right page
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
