//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool have_inf = false;
  bool have_evict = false;
  size_t mxtime = UINT64_MAX;
  for (auto [frame_id_, node] : node_store_) {
    if (!node.is_evictable_) {
      continue;
    }
    have_evict = true;
    if (node.history_.size() < k_) {
      if (!have_inf) {
        have_inf = true;
        mxtime = UINT64_MAX;
      }
      if (node.history_.front() < mxtime) {
        mxtime = node.history_.front();
        *frame_id = frame_id_;
      }
    } else if (!have_inf) {
      if (node.history_.front() < mxtime) {
        mxtime = node.history_.front();
        *frame_id = frame_id_;
      }
    }
  }
  if (have_evict) {
    node_store_.erase(*frame_id);
    --curr_size_;
  }
  return have_evict;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lock(latch_);
  // valid frame_id
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "frame id is invalid");

  // time+1
  ++current_timestamp_;

  // creat new node
  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode(frame_id);
  }
  //    auto &node = node_store_[frame_id];
  //    node.history_.push_back(current_timestamp_);
  //    if(node.history_.size() > k_) {
  //        node.history_.pop_front();
  //    }

  node_store_[frame_id].history_.push_back(current_timestamp_);
  if (node_store_[frame_id].history_.size() > k_) {
    node_store_[frame_id].history_.pop_front();
  }
  return void();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // valid frame_id
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, "frame id is invalid");
  if (node_store_.count(frame_id) == 0) {
    return void();
  }

  if (node_store_[frame_id].is_evictable_ == set_evictable) {
    return void();
  }

  curr_size_ += set_evictable ? 1 : -1;
  node_store_[frame_id].is_evictable_ = set_evictable;
  return void();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {
    return void();
  }
  BUSTUB_ASSERT(node_store_[frame_id].is_evictable_, "Remove is called on a non-evictable frame");

  node_store_.erase(frame_id);
  --curr_size_;
  return void();
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t curr_size = curr_size_;
  return curr_size;
}

}  // namespace bustub
