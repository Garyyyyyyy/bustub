#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  LOG_DEBUG("%d %d\n", leaf_max_size, internal_max_size);
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto root_id = GetRootPageId();
  return root_id == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // if empty
  if (IsEmpty()) {
    return false;
  }
  // find the leaf_id
  FindLeaf(key, ctx, txn);
  const auto leaf_page = ctx.read_set_.back().As<LeafPage>();
  return leaf_page->GetValue(key, result, comparator_, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafMut(const KeyType &key, Context &ctx, bool remove_flag, Transaction *txn) -> void {
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  ctx.root_page_id_ = ctx.header_page_->AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  page_id_t page_id = ctx.root_page_id_;
  // find the leaf_id

  ctx.index_.emplace_back(INVALID_PAGE_ID, 0, false);
  while (page_id != INVALID_PAGE_ID) {
    auto guard = bpm_->FetchPageWrite(page_id);
    auto type_page = guard.AsMut<BPlusTreePage>();
    ctx.write_set_.push_back(std::move(guard));
    if (type_page->IsLeafPage()) {
      return;
    }
    auto *page = ctx.write_set_.back().AsMut<InternalPage>();
    int size = page->GetSize();

    if (remove_flag) {
      // judge min > maxsize/2
      if (size > page->GetMinSize()) {
        ctx.header_page_->Drop();
        while (ctx.write_set_.size() != 1) {
          ctx.write_set_.pop_front();
        }
      }
    } else {
      if (comparator_(page->KeyAt(0), key) > 0) {
        page->SetKeyAt(0, key);
      }
      if (size + 1 < page->GetMaxSize()) {
        ctx.header_page_->Drop();
        while (ctx.write_set_.size() != 1) {
          ctx.write_set_.pop_front();
        }
      }
    }
    for (int id = 0; id < size; id++) {
      if (id == size - 1 || comparator_(key, page->KeyAt(id + 1)) < 0) {
        page_id = page->ValueAt(id);

        if (remove_flag) {
          if (id == size - 1) {
            ctx.index_.push_back({id == 0 ? INVALID_PAGE_ID : page->ValueAt(id - 1), id, false});
          } else {
            ctx.index_.push_back({page->ValueAt(id + 1), id, true});
          }
        }

        break;
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, Context &ctx, Transaction *txn) -> void {
  //    WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  //    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t page_id = header_page->root_page_id_;
  ctx.read_set_.push_back(std::move(header_guard));
  // find the leaf_id
  while (page_id != INVALID_PAGE_ID) {
    auto guard = bpm_->FetchPageRead(page_id);
    auto type_page = guard.As<BPlusTreePage>();
    ctx.read_set_.push_back(std::move(guard));
    ctx.read_set_.pop_front();
    if (type_page->IsLeafPage()) {
      return;
    }
    auto page = ctx.read_set_.back().As<InternalPage>();
    int size = page->GetSize();
    for (int id = 0; id < size; id++) {
      if (id == size - 1 || comparator_(key, page->KeyAt(id + 1)) < 0) {
        page_id = page->ValueAt(id);
        break;
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeaf(Context &ctx) -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t page_id = header_page->root_page_id_;
  ctx.read_set_.push_back(std::move(header_guard));
  // find the leaf_id
  while (page_id != INVALID_PAGE_ID) {
    auto guard = bpm_->FetchPageRead(page_id);
    auto type_page = guard.As<BPlusTreePage>();
    ctx.read_set_.push_back(std::move(guard));
    ctx.read_set_.pop_front();
    if (type_page->IsLeafPage()) {
      return ctx.read_set_.back().PageId();
    }
    auto page = ctx.read_set_.back().As<InternalPage>();
    page_id = page->ValueAt(0);
  }
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf_page, KeyType &split_key, page_id_t &split_page_id, Transaction *txn) {
  // creat new page
  NewLeafPage(split_page_id);
  WritePageGuard split_page_guard = bpm_->FetchPageWrite(split_page_id);
  auto split_page = split_page_guard.AsMut<LeafPage>();

  // split array
  split_key = leaf_page->Split(split_page);

  // modify page id
  split_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(split_page_id);
  // unpin new page
  bpm_->UnpinPage(split_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(InternalPage *internal_page, KeyType &split_key, page_id_t &split_page_id,
                                   Transaction *txn) {
  // creat new page
  NewInternalPage(split_page_id);
  WritePageGuard split_page_guard = bpm_->FetchPageWrite(split_page_id);
  auto split_page = split_page_guard.template AsMut<InternalPage>();

  // split array
  split_key = internal_page->Split(split_page);

  // unpin new page
  bpm_->UnpinPage(split_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::NewLeafPage(page_id_t &page_id, Transaction *txn) {
  // new page , remember to unpin
  bpm_->NewPage(&page_id);
  WritePageGuard write_guard = bpm_->FetchPageWrite(page_id);
  auto new_page = write_guard.AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::NewInternalPage(page_id_t &page_id, Transaction *txn) {
  // new page, remember to unpin
  bpm_->NewPage(&page_id);
  WritePageGuard write_guard = bpm_->FetchPageWrite(page_id);
  auto new_page = write_guard.AsMut<InternalPage>();
  new_page->Init(internal_max_size_ + 1);
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // if empty , add a new page
  {
    WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      page_id_t page_id;
      NewLeafPage(page_id, txn);
      bpm_->UnpinPage(page_id, true);
      SetRootPageId(header_page, page_id);
    }
  }

  // already have the key
  //  std::vector<ValueType> tmp;
  //  if (GetValue(key, &tmp, txn)) {
  //    return false;
  //  }

  FindLeafMut(key, ctx, 0, txn);
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  if (!leaf_page->Insert(key, value, comparator_)) {
    return false;
  }

  // if leaf page full
  Split(key, ctx);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Split(const KeyType &insert_key, Context &ctx) {
  page_id_t split_page_id;
  page_id_t last_page_id;
  KeyType last_key;
  KeyType split_key;

  // split leaf
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    return;
  }
  SplitLeaf(leaf_page, split_key, split_page_id);

  last_page_id = ctx.write_set_.back().PageId();
  last_key = leaf_page->KeyAt(0);
  ctx.write_set_.pop_back();

  while (!ctx.write_set_.empty()) {
    auto last_page = ctx.write_set_.back().AsMut<InternalPage>();
    last_page->Insert(split_key, split_page_id, comparator_);
    last_page_id = ctx.write_set_.back().PageId();
    last_key = last_page->KeyAt(0);
    //    if (comparator_(last_key, insert_key) > 0) {
    //      last_page->SetKeyAt(0, insert_key);
    //      last_key = insert_key;
    //    }
    if (last_page->GetSize() < last_page->GetMaxSize()) {
      return;
    }
    SplitInternal(last_page, split_key, split_page_id);
    ctx.write_set_.pop_back();
  }
  page_id_t root_id;
  NewInternalPage(root_id);
  SetRootPageId(ctx.header_page_->AsMut<BPlusTreeHeaderPage>(), root_id);

  WritePageGuard new_internal_page_guard = bpm_->FetchPageWrite(root_id);
  auto root_page = new_internal_page_guard.AsMut<InternalPage>();
  root_page->Insert(last_key, last_page_id, comparator_);
  root_page->Insert(split_key, split_page_id, comparator_);
  bpm_->UnpinPage(root_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  if (IsEmpty()) {
    return;
  }

//   already have the key
  std::vector<ValueType> tmp;
  if (!GetValue(key, &tmp, txn)) {
    return;
  }
  FindLeafMut(key, ctx, 1, txn);
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  leaf_page->Remove(key, comparator_);

  // if leaf page full
  Merge(ctx);
  //  LOG_DEBUG("Draw...\n");
  //  Draw(bpm_,"GGGG.txt");
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeaf(Context &ctx, std::vector<std::pair<KeyType, page_id_t>> *insert_record,
                               std::vector<int> *delete_record) {
  page_id_t l_page_id;
  page_id_t r_page_id;
  int l_index;
  int r_index;
  KeyType l_key;
  KeyType r_key;
  bool remove;
  {
    // merge leaf
    auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
    int leaf_page_size = leaf_page->GetSize();
    if (leaf_page_size >= leaf_page->GetMinSize()) {
      return;
    }
    auto [bro_page_id, self_index, right_bro] = ctx.index_.back();
    if (bro_page_id != INVALID_PAGE_ID) {
      WritePageGuard bro_page_guard = bpm_->FetchPageWrite(bro_page_id);
      auto bro_page = bro_page_guard.AsMut<LeafPage>();
      if (right_bro) {
        l_index = self_index;
        r_index = self_index + 1;
        l_page_id = ctx.write_set_.back().PageId();
        r_page_id = bro_page_guard.PageId();
        leaf_page->Merge(bro_page, l_key, r_key, remove, comparator_);
      } else {
        l_index = self_index - 1;
        r_index = self_index;
        r_page_id = ctx.write_set_.back().PageId();
        l_page_id = bro_page_guard.PageId();
        bro_page->Merge(leaf_page, l_key, r_key, remove, comparator_);
      }
      // must delete from right ,when using index to delete
      delete_record->emplace_back(r_index);
      delete_record->emplace_back(l_index);
      insert_record->emplace_back(l_key, l_page_id);
      if (!remove) {
        insert_record->emplace_back(r_key, r_page_id);
      }
    } else {
      // cause no brother , page_id is 0
      delete_record->emplace_back(0);

      if (leaf_page_size == 0) {
        // delete this page

        insert_record->clear();
      } else {
        insert_record->clear();
        insert_record->emplace_back(leaf_page->KeyAt(0), ctx.write_set_.back().PageId());
      }
    }
    ctx.write_set_.pop_back();
    ctx.index_.pop_back();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternal(Context &ctx, std::vector<std::pair<KeyType, page_id_t>> *insert_record,
                                   std::vector<int> *delete_record) {
  page_id_t l_page_id;
  page_id_t r_page_id;
  int l_index;
  int r_index;
  KeyType l_key;
  KeyType r_key;
  bool remove;

  auto last_page = ctx.write_set_.back().AsMut<InternalPage>();
  int last_page_size = last_page->GetSize();

  auto [bro_page_id, self_index, right_bro] = ctx.index_.back();
  if (bro_page_id != INVALID_PAGE_ID) {
    delete_record->clear();
    insert_record->clear();
    WritePageGuard bro_page_guard = bpm_->FetchPageWrite(bro_page_id);
    auto bro_page = bro_page_guard.AsMut<InternalPage>();
    if (right_bro) {
      l_index = self_index;
      r_index = self_index + 1;
      l_page_id = ctx.write_set_.back().PageId();
      r_page_id = bro_page_guard.PageId();
      last_page->Merge(bro_page, l_key, r_key, remove, comparator_);
    } else {
      l_index = self_index - 1;
      r_index = self_index;
      r_page_id = ctx.write_set_.back().PageId();
      l_page_id = bro_page_guard.PageId();
      bro_page->Merge(last_page, l_key, r_key, remove, comparator_);
    }
    // must delete from right ,when using index to delete
    delete_record->emplace_back(r_index);
    delete_record->emplace_back(l_index);
    insert_record->emplace_back(l_key, l_page_id);
    if (!remove) {
      insert_record->emplace_back(r_key, r_page_id);
    }
  } else {
    // cause no brother , page_id is 0
    delete_record->emplace_back(0);
    if (last_page_size == 0) {
      // delete this page
      insert_record->clear();
    } else if (last_page_size != 1) {
      // when size == 1, means this internal page only has one kid, so we can just delete this page ,and move the
      // kid's pointer to bottom delete this page

      insert_record->clear();
      insert_record->emplace_back(last_page->KeyAt(0), ctx.write_set_.back().PageId());
    }
  }
  ctx.write_set_.pop_back();
  ctx.index_.pop_back();
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(Context &ctx) {
  std::vector<std::pair<KeyType, page_id_t>> insert_record;
  std::vector<int> delete_record;

  MergeLeaf(ctx, &insert_record, &delete_record);

  while (!ctx.write_set_.empty()) {
    auto last_page = ctx.write_set_.back().AsMut<InternalPage>();
    for (auto id : delete_record) {
      last_page->Remove(id);
    }
    for (auto [key, page_id] : insert_record) {
      last_page->Insert(key, page_id, comparator_);
    }
    int last_page_size = last_page->GetSize();
    if (last_page_size >= last_page->GetMinSize()) {
      return;
    }

    MergeInternal(ctx, &insert_record, &delete_record);
  }

  if (!delete_record.empty() && insert_record.empty()) {
    SetRootPageId(ctx.header_page_->AsMut<BPlusTreeHeaderPage>(), INVALID_PAGE_ID);
    return;
  }
  if (!insert_record.empty()) {
    SetRootPageId(ctx.header_page_->AsMut<BPlusTreeHeaderPage>(), insert_record.begin()->second);
    return;
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_);
  }
  Context ctx;
  (void)ctx;
  page_id_t page_id = FindLeftMostLeaf(ctx);
  return INDEXITERATOR_TYPE(page_id, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id;
  int index;
  {
    Context ctx;
    (void)ctx;
    std::vector<ValueType> tmp;
    if (IsEmpty() || !GetValue(key, &tmp)) {
      return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_);
    }
    FindLeaf(key, ctx);
    auto leaf_page = ctx.read_set_.back().As<LeafPage>();
    page_id = ctx.read_set_.back().PageId();
    index = leaf_page->GetIndexByKey(key, comparator_);
  }
  return INDEXITERATOR_TYPE(page_id, index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, bpm_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  return header_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(BPlusTreeHeaderPage *page, page_id_t page_id) {
  // make sure header_page already fetch
  page->root_page_id_ = page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
