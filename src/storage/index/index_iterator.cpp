/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int index, BufferPoolManager *bpm)
    : page_id_(page_id), index_(index), bpm_(bpm) {
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  auto guard = bpm_->FetchPageRead(page_id);
  auto leaf_page = guard.As<LeafPage>();
  res_ = leaf_page->GetItem(index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> MappingType & { return res_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto guard = bpm_->FetchPageRead(page_id_);
  auto leaf_page = guard.template As<LeafPage>();
  int size = leaf_page->GetSize();
  if (index_ < size - 1) {
    //    LOG_DEBUG("%d %d\n", page_id_, index_);
    ++index_;
    res_ = leaf_page->GetItem(index_);
    return *this;
  }

  page_id_t next_page_id = leaf_page->GetNextPageId();

  index_ = 0;
  page_id_ = next_page_id;
  if (next_page_id != INVALID_PAGE_ID) {
    auto next_guard = bpm_->FetchPageRead(page_id_);
    auto next_page = next_guard.template As<LeafPage>();
    res_ = next_page->GetItem(index_);
  }
  //  LOG_DEBUG("%d %d\n", page_id_, index_);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
