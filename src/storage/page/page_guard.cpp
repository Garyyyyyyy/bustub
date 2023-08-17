#include "storage/page/page_guard.h"

#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  Drop();
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  page_ = that.page_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  page_ = that.page_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.page_ = that.guard_.page_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.page_ = that.guard_.page_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  guard_.page_->RUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.page_ = that.guard_.page_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  guard_.page_ = that.guard_.page_;

  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  guard_.page_->WUnlatch();
  guard_.bpm_ = nullptr;
  guard_.page_ = nullptr;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
