#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <set>
#include <condition_variable>
#include <memory>

#include "common/macros.h"
//#include "common/recursive_shared_mutex.h"

namespace buzzdb {

class BufferFrame {
 private:
	friend class BufferManager;

	uint64_t page_id;
	uint64_t frame_id;
	std::vector<char> data;

	bool dirty;
	bool exclusive;
	std::thread::id exclusive_thread_id;

 public:
	/// Returns a pointer to this page's data.
	char *get_data();

	BufferFrame();

	BufferFrame(const BufferFrame &other);

	BufferFrame &operator=(BufferFrame other);

	void mark_dirty() {dirty = true;}
};

class buffer_full_error : public std::exception {
 public:
	const char *what() const noexcept override { return "buffer is full"; }
};

class transaction_abort_error : public std::exception {
 public:
	const char *what() const noexcept override { return "transaction aborted"; }
};

class BufferManager {
 public:
	/// Constructor.
	/// @param[in] page_size  Size in bytes that all pages will have.
	/// @param[in] page_count Maximum number of pages that should reside in
	//                        memory at the same time.
	BufferManager(size_t page_size, size_t page_count);

	/// Destructor. Writes all dirty pages to disk.
	~BufferManager();

	BufferFrame &fix_page(uint64_t txn_id, uint64_t page_id, bool exclusive);


	void unfix_page(uint64_t txn_id, BufferFrame& page, bool is_dirty);

	/// Returns the segment id for a given page id which is contained in the 16
	/// most significant bits of the page id.
	static constexpr uint16_t get_segment_id(uint64_t page_id) {
		return page_id >> 48;
	}

	/// Returns the page id within its segment for a given page id. This
	/// corresponds to the 48 least significant bits of the page id.
	static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
		return page_id & ((1ull << 48) - 1);
	}

	/// Returns the overall page id associated with a segment id and
	/// a given segment page id.
	static uint64_t get_overall_page_id(uint16_t segment_id, uint64_t segment_page_id) {
		return (static_cast<uint64_t>(segment_id) << 48) | segment_page_id;
	}

	/// Print page id
	static std::string print_page_id(uint64_t page_id) {
		if (page_id == INVALID_NODE_ID) {
			return "INVALID";
		} else {
			auto segment_id = BufferManager::get_segment_id(page_id);
			auto segment_page_id = BufferManager::get_segment_page_id(page_id);
			return "( " + std::to_string(segment_id) + " " +
						 std::to_string(segment_page_id) + " )";
		}
	}

	size_t get_page_size() { return page_size_; }

	void flush_all_pages();

	void flush_page(uint64_t page_id);

	void discard_page(uint64_t page_id);

	void discard_all_pages();

	void flush_pages(uint64_t txn_id);

	void discard_pages(uint64_t txn_id);

	void transaction_complete(uint64_t txn_id);

	void transaction_abort(uint64_t txn_id);

	uint64_t get_frame_id_of_page(uint64_t page_id);

	bool deadlock_detection(uint64_t txn_id, uint64_t page_id, bool exclusive);

	bool uf(uint64_t v, bool visited[], bool *recStack);

	bool isCyclic();

 private:
	uint64_t capacity_;
	size_t page_size_;
	uint64_t page_counter_ = 0;

	class GrantedTxns {
		public:
			GrantedTxns(bool exclusive, uint64_t txn_id)
				: lock_type(exclusive), granted_set_({txn_id}) {};

			// type of the lock granted
			bool lock_type;
			// a set of txns that we granted this lock
			std::set<uint64_t> granted_set_;
    };

	class Transaction{
		public:
			uint64_t txn_id_ = INVALID_TXN_ID;
    		bool started_ = false;
			std::set<uint64_t> modified_pages_;

			Transaction():
				txn_id_(INVALID_TXN_ID),
				started_(false){
			}

			Transaction(uint64_t txn_id, bool started):
							txn_id_(txn_id),
							started_(started){
			}
	};

	std::map<uint64_t, Transaction> transaction_table_;

	std::vector<std::unique_ptr<BufferFrame>> pool_;

	std::unordered_map<uint64_t, std::shared_ptr<GrantedTxns>> lock_table_;

	std::unordered_map<uint64_t, std::shared_ptr<std::condition_variable>> cv_table_;

	mutable std::mutex file_use_mutex_;

	mutable std::mutex mutex_;
	
	void read_frame(uint64_t frame_id);

	void write_frame(uint64_t frame_id);

	std::vector<uint64_t> graph[5];

};

}  // namespace buzzdb
