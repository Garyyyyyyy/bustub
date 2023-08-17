//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  EXPECT_EQ(1, page0->GetPinCount());
  // test ~ReadPageGuard()
  {
    auto basic_guard = bpm->FetchPageBasic(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  {
    auto writer_guard = bpm->FetchPageWrite(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  auto *page1 = bpm->NewPage(&page_id_temp);
  auto *page2 = bpm->NewPage(&page_id_temp);
  auto *page3 = bpm->NewPage(&page_id_temp);
  auto *page4 = bpm->NewPage(&page_id_temp);
  auto *page5 = bpm->NewPage(&page_id_temp);
  LOG_DEBUG("HH %d\n", page_id_temp);
  EXPECT_EQ(1, page1->GetPinCount());
  EXPECT_EQ(1, page2->GetPinCount());
  EXPECT_EQ(1, page3->GetPinCount());
  EXPECT_EQ(1, page4->GetPinCount());
  EXPECT_EQ(nullptr, page5);

  EXPECT_EQ(1, page0->GetPinCount());
  // test ReadPageGuard(ReadPageGuard &&that)
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(1, page0->GetPinCount());
    auto reader_guard_2 = ReadPageGuard(std::move(reader_guard));
    EXPECT_EQ(1, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test ReadPageGuard::operator=(ReadPageGuard &&that)
  {
    auto reader_guard_1 = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_1 = std::move(reader_guard_2);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  //  const std::string db_name = "test.db";
  //  const size_t buffer_pool_size = 5;
  //  const size_t k = 2;
  //
  //  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  //  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size,
  //  disk_manager.get(), k);
  //
  //  page_id_t page_id_temp;
  //  auto *page0 = bpm->NewPage(&page_id_temp);
  //
  //  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  //
  //  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  //  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  //  EXPECT_EQ(1, page0->GetPinCount());
  //
  //  guarded_page.Drop();
  //
  //  EXPECT_EQ(0, page0->GetPinCount());
  //
  //  // Shutdown the disk manager and remove the temporary file we created.
  //  disk_manager->ShutDown();
}

}  // namespace bustub
