
#include <cassert>
#include <iostream>
#include <string>
#include <list>
#include <unordered_set>

#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"
#include <chrono>
#include <ctime> 

uint64_t wake_timeout_ = 100;
uint64_t timeout_ = 2;

namespace buzzdb {

char* BufferFrame::get_data() { return data.data(); }

BufferFrame::BufferFrame()
    : page_id(INVALID_PAGE_ID),
      frame_id(INVALID_FRAME_ID),
      dirty(false),
      exclusive(false) {}

BufferFrame::BufferFrame(const BufferFrame& other)
    : page_id(other.page_id),
      frame_id(other.frame_id),
      data(other.data),
      dirty(other.dirty),
      exclusive(other.exclusive) {}

BufferFrame& BufferFrame::operator=(BufferFrame other) {
  std::swap(this->page_id, other.page_id);
  std::swap(this->frame_id, other.frame_id);
  std::swap(this->data, other.data);
  std::swap(this->dirty, other.dirty);
  std::swap(this->exclusive, other.exclusive);
  return *this;
}

BufferManager::BufferManager(size_t page_size, size_t page_count) {
  capacity_ = page_count;
  page_size_ = page_size;

  pool_.resize(capacity_);
  for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
    pool_[frame_id].reset(new BufferFrame());
    pool_[frame_id]->data.resize(page_size_);
    pool_[frame_id]->frame_id = frame_id;
    pool_[frame_id]->page_id = INVALID_PAGE_ID;
  }
}

BufferManager::~BufferManager() {
}

bool BufferManager::uf(uint64_t v, bool visited[], bool *recStack)
{
    if(visited[v] == false)
    {
        // Mark the current node as visited and part of recursion stack
        visited[v] = true;
        recStack[v] = true;
 
        // Recur for all the vertices adjacent to this vertex
        std::vector<uint64_t>::iterator i;
        for(i = graph[v].begin(); i != graph[v].end(); ++i)
        {
            if ( !visited[*i] && uf(*i, visited, recStack) )
                return true;
            else if (recStack[*i])
                return true;
        }
 
    }
    recStack[v] = false;  // remove the vertex from recursion stack
    return false;
}

bool BufferManager::isCyclic()
{
    // Mark all the vertices as not visited and not part of recursion
    // stack
    bool *visited = new bool[5];
    bool *recStack = new bool[5];
    for(int i = 0; i < 5; i++)
    {
        visited[i] = false;
        recStack[i] = false;
    }
 
    // Call the recursive helper function to detect cycle in different
    // DFS trees
    for(int i = 0; i < 5; i++){
      if ( !visited[i] && uf(i, visited, recStack)){
        delete[] visited;
        delete[] recStack;
        return true;
      }
    }
        
    delete[] visited;
    delete[] recStack;        
    
    return false;
}

bool BufferManager::deadlock_detection(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE uint64_t page_id, UNUSED_ATTRIBUTE bool exclusive){
  if (lock_table_.find(page_id) != lock_table_.end()){
    auto grantedTxns = lock_table_[page_id];
    if((grantedTxns->lock_type == false && exclusive == false) || grantedTxns->granted_set_.find(txn_id) != grantedTxns->granted_set_.end()){
      return false;
    }
    else{
      for (auto itr : grantedTxns->granted_set_){
        graph[txn_id].push_back(itr);
      }

      // std::sort(graph[txn_id].begin(), graph[txn_id].end()); 
      // auto last = std::unique(graph[txn_id].begin(), graph[txn_id].end());
      // graph[txn_id].erase(last, graph[txn_id].end());

      std::unordered_set<uint64_t> s(graph[txn_id].begin(), graph[txn_id].end());
      graph[txn_id].assign(s.begin(), s.end());

      return isCyclic();
    }

  }
  else{
    return false;
  }
}

BufferFrame& BufferManager::fix_page(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE uint64_t page_id, UNUSED_ATTRIBUTE bool exclusive) {
    
  if (page_id == INVALID_PAGE_ID) {
    std::cout << "INVALID FIX REQUEST \n";
    exit(-1);
  }

  if(deadlock_detection(txn_id, page_id, exclusive)){
    //std::cout << "hello world\n";
    transaction_abort(txn_id);
    throw transaction_abort_error();
  }

  uint64_t page_frame_id = get_frame_id_of_page(page_id);
  
  if (page_frame_id != INVALID_FRAME_ID) {    

    if (lock_table_.find(page_id) != lock_table_.end()){
      

      if ((lock_table_[page_id]->lock_type == false && exclusive == false) || lock_table_[page_id]->granted_set_.find(txn_id) != lock_table_[page_id]->granted_set_.end()){
        std::unique_lock<std::mutex> lock(mutex_);
        lock_table_[page_id]->granted_set_.insert(txn_id);
        if (transaction_table_.find(txn_id) == transaction_table_.end()){
          Transaction txn(txn_id, true);
          transaction_table_.insert({txn_id, txn});  
        }
        else{
          auto &txn = transaction_table_[txn_id];
          txn.modified_pages_.insert(page_id);
        }
        return *pool_[page_frame_id];

      }
      else{
        auto cv = cv_table_[page_id];
        // std::clock_t start;
        // start = std::clock();
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv->wait_for(lock, std::chrono::milliseconds(10), [&] { return lock_table_.find(page_id) == lock_table_.end() || (lock_table_[page_id]->lock_type == false && exclusive == false);})){
          
          if (lock_table_.find(page_id) == lock_table_.end()){
            lock_table_.emplace(page_id, std::make_shared<GrantedTxns>(exclusive, txn_id));
          }
          else if(lock_table_[page_id]->lock_type == false && exclusive == false){
            lock_table_[page_id]->granted_set_.insert(txn_id);
          }

          if (transaction_table_.find(txn_id) == transaction_table_.end()){
            Transaction txn(txn_id, true);
            transaction_table_.insert({txn_id, txn});  
          }
          else{
            auto &txn = transaction_table_[txn_id];
            txn.modified_pages_.insert(page_id);
          }

          cv->notify_all();
          
          return *pool_[page_frame_id];
        }
        else{
          // std::cout << "hello world\n";
          transaction_abort(txn_id);
          throw transaction_abort_error();  
        }
        // cv->wait(lock, [&] { return lock_table_.find(page_id) == lock_table_.end() || (lock_table_[page_id]->lock_type == false && exclusive == false) || (std::clock() - start) / (double) CLOCKS_PER_SEC > time_out; });
        
        // if((std::clock() - start) / (double) CLOCKS_PER_SEC > time_out){
        //   transaction_abort(txn_id);
        //   throw transaction_abort_error();
        // }

 
      }
    }
    else{
      lock_table_.emplace(page_id, std::make_shared<GrantedTxns>(exclusive, txn_id));

      if (cv_table_.find(page_id) == cv_table_.end()){
        cv_table_.emplace(page_id, std::make_shared<std::condition_variable>());
      }  
      std::unique_lock<std::mutex> lock(mutex_);
      if (transaction_table_.find(txn_id) == transaction_table_.end()){
        Transaction txn(txn_id, true);
        transaction_table_.insert({txn_id, txn});  
      }
      else{
        auto &txn = transaction_table_[txn_id];
        txn.modified_pages_.insert(page_id);
      }
      return *pool_[page_frame_id];
    }
    
  }

  uint64_t free_frame_id = page_counter_++;

  if(page_counter_ >= capacity_){
    std::cout << "Out of space \n";
    std::cout << page_counter_ << " " << capacity_ << "\n";
    exit(-1);
  }

  pool_[free_frame_id]->page_id = page_id;
  pool_[free_frame_id]->dirty = false;

  read_frame(free_frame_id);
  lock_table_.emplace(page_id, std::make_shared<GrantedTxns>(exclusive, txn_id));

  if (cv_table_.find(page_id) == cv_table_.end()){
    cv_table_.emplace(page_id, std::make_shared<std::condition_variable>());
  }  
  std::unique_lock<std::mutex> lock(mutex_);
  if (transaction_table_.find(txn_id) == transaction_table_.end()){
    Transaction txn(txn_id, true);
    transaction_table_.insert({txn_id, txn}); 
    
    transaction_table_[txn_id].modified_pages_.insert(page_id);
  }
  else{
    auto &txn = transaction_table_[txn_id];
    txn.modified_pages_.insert(page_id);
  }
  
  return *pool_[free_frame_id];
}

void BufferManager::unfix_page(UNUSED_ATTRIBUTE uint64_t txn_id, UNUSED_ATTRIBUTE BufferFrame& page, UNUSED_ATTRIBUTE bool is_dirty) {
  uint64_t page_id = page.page_id;
  auto cv = cv_table_[page_id];
  auto grantedTxns = lock_table_[page_id];
  std::unique_lock<std::mutex> lock(mutex_);

  grantedTxns->granted_set_.erase(txn_id);
  if (grantedTxns->granted_set_.empty()) {
      lock_table_.erase(page_id);
  }

  if (!page.dirty) {
		page.dirty = is_dirty;
	}

  cv->notify_all(); 
  return;
}

void BufferManager::flush_page(uint64_t page_id){
  uint64_t page_frame_id = get_frame_id_of_page(page_id);
	if (page_frame_id != INVALID_FRAME_ID) {
		if (pool_[page_frame_id]->dirty == true) {
			write_frame(page_frame_id);
		}
	}
}

void BufferManager::discard_page(uint64_t page_id){
  uint64_t page_frame_id = get_frame_id_of_page(page_id);
  if (page_frame_id != INVALID_FRAME_ID) {
    pool_[page_frame_id].reset(new BufferFrame());
    pool_[page_frame_id]->page_id = INVALID_PAGE_ID;
    pool_[page_frame_id]->dirty = false;
    pool_[page_frame_id]->data.resize(page_size_);
  } 
}

void  BufferManager::flush_all_pages(){

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->dirty == true) {
			write_frame(frame_id);
		}
	}

}

void  BufferManager::discard_all_pages(){

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		pool_[frame_id].reset(new BufferFrame());
		pool_[frame_id]->page_id = INVALID_PAGE_ID;
		pool_[frame_id]->dirty = false;
		pool_[frame_id]->data.resize(page_size_);
	}

}

void BufferManager::discard_pages(UNUSED_ATTRIBUTE uint64_t txn_id){
  // Discard all pages acquired by the transaction 
  auto& txn = transaction_table_[txn_id];

  for(auto page_id : txn.modified_pages_){
    discard_page(page_id);
  }

}

void BufferManager::flush_pages(UNUSED_ATTRIBUTE uint64_t txn_id){
    // Flush all dirty pages acquired by the transaction to disk
    auto& txn = transaction_table_[txn_id];

    for(auto page_id : txn.modified_pages_){
      flush_page(page_id);
    }
}

void BufferManager::transaction_complete(UNUSED_ATTRIBUTE uint64_t txn_id){
  // Free all the locks acquired by the transaction
  // std::cout << "hello\n";
  flush_pages(txn_id);
  
  auto& txn = transaction_table_[txn_id];

  // for (auto& it: transaction_table_) {
  //     // Do stuff
  //     std::cout << it.first << "\n";
  // }

  for(auto page_id : txn.modified_pages_){
    // std::cout << txn_id << " " << page_id << "\n";

    

    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (pool_[frame_id]->page_id == page_id) {
        
        BufferFrame& page = *pool_[frame_id];
        unfix_page(txn_id, page, page.dirty);
      }
	  }    
  }
  graph[txn_id].clear();
  transaction_table_.erase(txn_id);
}

void BufferManager::transaction_abort(UNUSED_ATTRIBUTE uint64_t txn_id){
  // Free all the locks acquired by the transaction
  discard_pages(txn_id);
  auto& txn = transaction_table_[txn_id];

  for(auto page_id : txn.modified_pages_){
    for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
      if (pool_[frame_id]->page_id == page_id) {
        BufferFrame& page = *pool_[frame_id];
        unfix_page(txn_id, page, page.dirty);
      }
	  }    
  }
  graph[txn_id].clear();
  transaction_table_.erase(txn_id);
}


void BufferManager::read_frame(uint64_t frame_id) {
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->read_block(start, page_size_, pool_[frame_id]->data.data());
}

void BufferManager::write_frame(uint64_t frame_id) {
  std::lock_guard<std::mutex> file_guard(file_use_mutex_);

  auto segment_id = get_segment_id(pool_[frame_id]->page_id);
  auto file_handle =
      File::open_file(std::to_string(segment_id).c_str(), File::WRITE);
  size_t start = get_segment_page_id(pool_[frame_id]->page_id) * page_size_;
  file_handle->write_block(pool_[frame_id]->data.data(), start, page_size_);
}

uint64_t BufferManager::get_frame_id_of_page(uint64_t page_id){

	uint64_t page_frame_id = INVALID_FRAME_ID;

	for (size_t frame_id = 0; frame_id < capacity_; frame_id++) {
		if (pool_[frame_id]->page_id == page_id) {
			page_frame_id = frame_id;
			break;
		}
	}

	return page_frame_id;
}

}  // namespace buzzdb
