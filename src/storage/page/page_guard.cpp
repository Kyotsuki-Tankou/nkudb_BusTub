#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  // std::swap(bpm_, that.bpm_);
  // std::swap(page_, that.page_);
  // std::swap(is_dirty_, that.is_dirty_);
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  this->page_ = that.page_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // check self-point
  if (this == &that) {
    return *this;
  }

  // drop&unpin currentl page
  Drop();

  // move other's to this
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  // reset other's
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

// // auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return {bpm_, page_}; }

// // auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  this->page_->RLatch();
  ReadPageGuard read_guard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();
  WritePageGuard write_guard(this->bpm_, this->page_);
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
  return write_guard;
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {
  if (page == nullptr) return;
  guard_.bpm_ = bpm;
  guard_.page_ = page;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // latch_->RUnlock();
    Drop();
    guard_ = std::move(that.guard_);
    // latch_ = std::move(that.latch_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  auto &guard = this->guard_;
  if (guard.page_ != nullptr) {
    guard.bpm_->UnpinPage(this->PageId(), this->guard_.is_dirty_);
    guard.page_->RUnlatch();
  }
  guard.page_ = nullptr;
  guard.bpm_ = nullptr;
  guard.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() {Drop();}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {
  if (page == nullptr) return;
  guard_.bpm_ = bpm;
  guard_.page_ = page;
}
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept{
  this->guard_ = std::move(that.guard_);
  // latch_->WLock();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    // latch_->WUnlock();
    guard_ = std::move(that.guard_);
    // latch_ = std::move(that.latch_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  auto &guard = this->guard_;
  if (guard.page_ != nullptr) {
    guard.bpm_->UnpinPage(this->PageId(), this->guard_.is_dirty_);
    guard.page_->WUnlatch();
  }
  guard.page_ = nullptr;
  guard.bpm_ = nullptr;
  guard.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub