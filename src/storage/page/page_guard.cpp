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

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
