//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool
  // manager
  //  throw NotImplementedException(
  //      "BufferPoolManager is not implemented yet. If you have finished
  //      implementing BPM, please remove the throw " "exception line in
  //      `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  *page_id = INVALID_PAGE_ID;
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    *page_id = AllocatePage();
    frame_id = free_list_.front();
    free_list_.pop_front();

    page_table_[*page_id] = frame_id;

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].ResetMemory();

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (replacer_->Evict(&frame_id)) {
    // if dirty page , write back
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    //    replacer_->Remove(frame_id);

    *page_id = AllocatePage();

    page_table_.erase(pages_[frame_id].page_id_);
    page_table_[*page_id] = frame_id;

    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].ResetMemory();

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  *page_id = INVALID_PAGE_ID;
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  frame_id_t frame_id;
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];

    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();

    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    page_table_[page_id] = frame_id;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    return &pages_[frame_id];
  }
  if (replacer_->Evict(&frame_id)) {
    // if dirty page , write back
    if (pages_[frame_id].is_dirty_) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    }
    //    replacer_->Remove(frame_id);

    page_table_.erase(pages_[frame_id].page_id_);
    page_table_[page_id] = frame_id;

    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 1;
    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (page_table_.count(page_id) != 0 && pages_[frame_id = page_table_[page_id]].pin_count_ != 0) {
    --pages_[frame_id].pin_count_;
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    pages_[frame_id].is_dirty_ |= is_dirty;
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPage(pages_[i].page_id_);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];

  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  }
  replacer_->Remove(frame_id);

  page_table_.erase(page_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();

  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  //  std::scoped_lock<std::mutex> lock(latch_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  //  std::scoped_lock<std::mutex> lock(latch_);
  return {this, FetchPage(page_id)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  //  std::scoped_lock<std::mutex> lock(latch_);
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  //  std::scoped_lock<std::mutex> lock(latch_);
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  //  std::scoped_lock<std::mutex> lock(latch_);
  return {this, NewPage(page_id)};
}

}  // namespace bustub
