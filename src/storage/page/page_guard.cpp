#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::swap(bpm_, that.bpm_);
  std::swap(page_, that.page_);
  std::swap(is_dirty_, that.is_dirty_);
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }
  if (is_dirty_) {
    page_->SetDirty();
    is_dirty_=true;
  }

  //Unpin
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);

  //Reset
  page_ = nullptr;
  bpm_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  //check self-point
  if (this == &that) {
    return *this;
  }

  //drop&unpin currentl page
  Drop();

  //move other's to this
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;

  //reset other's
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}


BasicPageGuard::~BasicPageGuard(){ Drop();};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return {bpm_, page_}; }

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { return {bpm_, page_}; }

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept:guard_(std::move(that.guard_)) {latch_->RLock();}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    latch_->RUnlock();
    guard_ = std::move(that.guard_);
    latch_ = std::move(that.latch_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  latch_->RUnlock();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page):guard_(bpm, page), latch_(new ReaderWriterLatch()) {
  latch_->WLock();
}
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept:guard_(std::move(that.guard_)), latch_(std::move(that.latch_)) {
  latch_->WLock();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    latch_->WUnlock();
    guard_ = std::move(that.guard_);
    latch_ = std::move(that.latch_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  latch_->WUnlock();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {Drop();}  // NOLINT

}  // namespace bustub
