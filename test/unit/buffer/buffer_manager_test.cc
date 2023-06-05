#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <future>

#include "buffer/buffer_manager.h"
#include "heap/heap_file.h"
#include "transaction/transaction_manager.h"
#include "log/log_manager.h"

using buzzdb::LogManager;
using buzzdb::BufferManager;
using buzzdb::HeapSegment;
using buzzdb::TransactionManager;
using buzzdb::TID;
using buzzdb::BufferFrame;
using buzzdb::SlottedPage;
using buzzdb::File;

const char* LOG_FILE = buzzdb::LOG_FILE_PATH.c_str();
const uint64_t WAIT_INTERVAL = 2;
const uint64_t TIMEOUT = 1;

namespace {

class LockGrabber{
  private:
    bool acquired;
    bool thread_done;
	bool err;
	std::future<std::tuple<BufferFrame, bool>> fut;
	//BufferFrame& page;
  public:
	LockGrabber(buzzdb::BufferManager &buffer_manager, uint64_t txn_id, uint64_t page_id, bool exclusive){
		acquired = false;
		thread_done = false;
		err = false;
		//page = BufferFrame();
		fut = grab_lock(buffer_manager, txn_id, page_id, exclusive);
	}
  
	std::future<std::tuple<BufferFrame, bool>> grab_lock(buzzdb::BufferManager &buffer_manager, uint64_t txn_id, uint64_t page_id, bool exclusive){
		std::future<std::tuple<BufferFrame, bool>> future = std::async(std::launch::async, 
		[&buffer_manager, txn_id, page_id, exclusive](){ 
			try{
			auto& page = buffer_manager.fix_page(txn_id, page_id, exclusive);
			return std::make_tuple(page, false);
			}
			catch (const buzzdb::transaction_abort_error&) {
				return std::make_tuple(BufferFrame(), true);
				}   
		});
    	return future;
	}
	

	bool is_ready()
	{ return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready; }

	bool is_acquired(){ 
		if (!thread_done && is_ready()) { 
			auto tup = fut.get();
			err = std::get<1>(tup);
			//page = std::get<0>(tup);
			thread_done = true;
			acquired = !err;
		}
		
		return acquired;
	}

	bool error(){
		return err;
	}
};

void clean_up(LockGrabber* lg1, uint64_t t1, LockGrabber* lg2, uint64_t t2, BufferManager& buffer_manager){

    if(lg1->is_acquired()){
      buffer_manager.transaction_complete(t1);
      sleep(WAIT_INTERVAL);
      buffer_manager.transaction_complete(t2);
    }
    else if (lg2->is_acquired()){
      buffer_manager.transaction_complete(t2);
      sleep(WAIT_INTERVAL);
      buffer_manager.transaction_complete(t1);
    }

}

// TID insert_row(HeapSegment& heap_segment,
// 		UNUSED_ATTRIBUTE TransactionManager& transaction_manager,
// 		UNUSED_ATTRIBUTE uint64_t txn_id,
// 		uint64_t table_id, uint64_t field){

// 	auto tuple_size = sizeof(uint64_t)*2; // table_id | field

// 	// Allocate slot
// 	auto tid = heap_segment.allocate(tuple_size, txn_id);

// 	// Write buffer
// 	std::vector<char> buf;
// 	buf.resize(tuple_size);
// 	memcpy(buf.data() + 0, &table_id, sizeof(uint64_t));
// 	memcpy(buf.data() + sizeof(uint64_t), &field, sizeof(uint64_t));

// 	heap_segment.write(tid, reinterpret_cast<std::byte *>(buf.data()),
// 			tuple_size, txn_id);
//   return tid;
// }

// // Check whether the specified tuple is, or is not, present
// bool look(HeapSegment& heap_segment,
//             uint64_t txn_id,
//             BufferManager& buffer_manager,
//             UNUSED_ATTRIBUTE uint64_t table_id, uint64_t expected_field,
//             bool should_be_present){

//         auto tuple_size = sizeof(uint64_t)*2; // table_id | field
//         size_t count = 0;

//         // Go over all pages
//         for (size_t segment_page_itr = 0;
//                 segment_page_itr < heap_segment.page_count_;
//                 segment_page_itr++) {

//                 uint64_t page_id =
//                         BufferManager::get_overall_page_id(
//                                 heap_segment.segment_id_, segment_page_itr);

//                 BufferFrame &frame = buffer_manager.fix_page(txn_id, page_id, false);
//                 auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());
//                 page->header.buffer_frame = reinterpret_cast<char*>(page);
//                 auto overall_page_id = page->header.overall_page_id;
//                 auto slot_count = page->header.first_free_slot;

//                 // Go over all slots in page
//                 for(size_t slot_itr = 0;
//                         slot_itr < slot_count;
//                         slot_itr++){
//                     TID tid = TID(overall_page_id, slot_itr);

//                     // Check slot
//                     std::vector<char> buf;
//                     buf.resize(tuple_size);
//                     uint64_t table_id, field;
//                     heap_segment.read(tid, reinterpret_cast<std::byte *>(buf.data()),
//                             tuple_size, txn_id);
                
//                     memcpy(&table_id, buf.data() + 0, sizeof(uint64_t));
//                     memcpy(&field, buf.data() + sizeof(uint64_t), sizeof(uint64_t));

//                     if(field == expected_field){
//                         count = count + 1;
//                     }
//                 }

//             }

//         // tuple repeated
//         if(count > 1){
//             return false;
//         }
//         // tuple missing
//         else if(count == 0 && should_be_present == true){
//             return false;
//         }
//         // tuple missing
//         else if(count > 0 && should_be_present == false){
//             return false;
//         }

//         return true;
//     }


/*
  Test Cases
*/

// TEST(BufferManagerTest, AcquireReadLocksOnSamePage) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, false);
//     auto lock = LockGrabber(buffer_manager, 2, 1, false);
// 	sleep(TIMEOUT);
//     EXPECT_TRUE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }

// TEST(BufferManagerTest, AcquireReadWriteLocksOnSamePage) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, false);
//     auto lock = LockGrabber(buffer_manager, 2, 1, true);
// 	sleep(TIMEOUT);
//     EXPECT_FALSE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }

// TEST(BufferManagerTest, AcquireWriteReadLocksOnSamePage) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, true);
//     auto lock = LockGrabber(buffer_manager, 2, 1, false);
// 	sleep(TIMEOUT);
//     EXPECT_FALSE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }

// TEST(BufferManagerTest, AcquireReadLocksOnTwoPages) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, false);
//     auto lock = LockGrabber(buffer_manager, 2, 2, false);
// 	sleep(TIMEOUT);
//     EXPECT_TRUE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }

// TEST(BufferManagerTest, AcquireWriteReadLocksOnTwoPages) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, true);
//     auto lock = LockGrabber(buffer_manager, 2, 2, false);
// 	sleep(TIMEOUT);
//     EXPECT_TRUE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }

// TEST(BufferManagerTest, AcquireWriteLocksOnTwoPages) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, true);
//     auto lock = LockGrabber(buffer_manager, 2, 2, true);
// 	sleep(TIMEOUT);
//     EXPECT_TRUE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }

// TEST(BufferManagerTest, AcquireWriteAndReadLocks) {
//   buzzdb::BufferManager buffer_manager{1024, 10};
//     auto& page = buffer_manager.fix_page(1, 1, true);
//     auto lock = LockGrabber(buffer_manager, 1, 1, false);
// 	  sleep(TIMEOUT);
//     EXPECT_TRUE(lock.is_acquired());
//     buffer_manager.unfix_page(1, page, true);
// }


// /*
//    * Try to acquire locks that would conflict if old locks aren't released
//    * during transactionComplete()
// */
// TEST(BufferManagerTransactionTest, AttemptTransactionTwice) {
//     buzzdb::BufferManager buffer_manager{1024, 10};
//     buffer_manager.fix_page(1, 1, false);
//     buffer_manager.fix_page(1, 2, true);

//     buffer_manager.transaction_complete(1);
    
//     auto lock = LockGrabber(buffer_manager, 2, 1, true);
//     auto lock_1 = LockGrabber(buffer_manager, 2, 2, true);
// 	  sleep(TIMEOUT);

//     EXPECT_TRUE(lock.is_acquired());
//     EXPECT_TRUE(lock_1.is_acquired());
    
// }

// /**
//    * Verify that a tuple inserted during a committed transaction is durable
//    */
// TEST(BufferManagerTransactionTest, AbortTransaction){
//   BufferManager buffer_manager(128, 10);
//   auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
//   LogManager log_manager(logfile.get());
//   HeapSegment heap_segment(124, log_manager, buffer_manager);
// 	TransactionManager transaction_manager(log_manager, buffer_manager);
//   buffer_manager.discard_all_pages();
//   buffer_manager.flush_all_pages();
// 	uint64_t table_id = 101;
//   insert_row(heap_segment, transaction_manager, 1, table_id, 5);
//   buffer_manager.transaction_abort(1);

//   // reset buffer manager and verify the tuple should not be on disk
//   buffer_manager.discard_all_pages();
//   uint64_t txn_id = 2;
//   EXPECT_TRUE(look(heap_segment, txn_id, buffer_manager,
// 			table_id, 5, false));
// }


// /**
//    * Verify that a tuple inserted during a committed transaction is durable
//    */
// TEST(BufferManagerTransactionTest, CommitTransaction){
//   BufferManager buffer_manager(128, 10);
//   auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
//   LogManager log_manager(logfile.get());
//   HeapSegment heap_segment(123, log_manager, buffer_manager);
// 	TransactionManager transaction_manager(log_manager, buffer_manager);
//   buffer_manager.discard_all_pages();
// 	uint64_t table_id = 101;
//   insert_row(heap_segment, transaction_manager, 1, table_id, 5);
// //   std::cout << "hello\n";
//   buffer_manager.transaction_complete(1);

//   // reset buffer manager and verify if tuple is on disk
//   buffer_manager.discard_all_pages();
//   uint64_t txn_id = 2;
//   EXPECT_TRUE(look(heap_segment, txn_id, buffer_manager,
// 			table_id, 5, true));
// }


// TEST(DeadlockTest, ReadWriteDeadlock){
//   BufferManager buffer_manager(128, 10);
//   uint64_t p1 = 1, p2 = 2;
//   uint64_t t1 = 1, t2 = 2;
 
//   LockGrabber* lg1_read = new LockGrabber(buffer_manager, t1, p1, false);
//   LockGrabber* lg2_read = new LockGrabber(buffer_manager, t2, p2, false);

//   sleep(TIMEOUT);


//   // deadlock
//   LockGrabber* lg1_write = new LockGrabber(buffer_manager, t1, p2, true); 
//   LockGrabber* lg2_write = new LockGrabber(buffer_manager, t2, p1, true);

//   while(1){
//     EXPECT_FALSE(lg1_write->is_acquired() && lg2_write->is_acquired());
//     if(lg1_write->is_acquired() && !lg2_write->is_acquired()) break;
//     if(!lg1_write->is_acquired() && lg2_write->is_acquired()) break;

//     if(lg1_write->error()){
//       buffer_manager.transaction_abort(t1);
// 	  delete lg1_read; delete lg1_write;
//       sleep(WAIT_INTERVAL);
//       lg1_read = new LockGrabber(buffer_manager, t1, p1, false);
//       lg1_write = new LockGrabber(buffer_manager, t1, p2, true);
//     }

//     if(lg2_write->error()){
//       buffer_manager.transaction_abort(t2);
// 	  delete lg2_read; delete lg2_write;
//       sleep(WAIT_INTERVAL);
//       lg2_read = new LockGrabber(buffer_manager, t2, p2, false);
//       lg2_write = new LockGrabber(buffer_manager, t2, p1, true);
//     }
//   }
  

//   clean_up(lg1_write, t1, lg2_write, t2, buffer_manager);

//   delete lg1_read; delete lg2_read; delete lg1_write; delete lg2_write;
// }


TEST(DeadlockTest, WriteWriteDeadlock){
  BufferManager buffer_manager(128, 10);
  uint64_t p1 = 1, p2 = 2;
  uint64_t t1 = 1, t2 = 2;
 
  LockGrabber* lg1_write = new LockGrabber(buffer_manager, t1, p1, true);
  LockGrabber* lg2_write = new LockGrabber(buffer_manager, t2, p2, true);


  sleep(TIMEOUT);


  // deadlock
  LockGrabber* lg1_write1 = new LockGrabber(buffer_manager, t1, p2, true); 
  LockGrabber* lg2_write1 = new LockGrabber(buffer_manager, t2, p1, true);

  while(1){
    EXPECT_FALSE(lg1_write1->is_acquired() && lg2_write1->is_acquired());
    if(lg1_write1->is_acquired() && !lg2_write1->is_acquired()) break;
    if(!lg1_write1->is_acquired() && lg2_write1->is_acquired()) break;

    if(lg1_write1->error()){
      buffer_manager.transaction_abort(t1);
	    delete lg1_write; delete lg1_write1;
      sleep(WAIT_INTERVAL);
      lg1_write = new LockGrabber(buffer_manager, t1, p1, true);
      lg1_write1 = new LockGrabber(buffer_manager, t1, p2, true);
    }

    if(lg2_write1->error()){
      buffer_manager.transaction_abort(t2);
	    delete lg2_write1; delete lg2_write;
      sleep(WAIT_INTERVAL);
      lg2_write = new LockGrabber(buffer_manager, t2, p2, true);
      lg2_write1 = new LockGrabber(buffer_manager, t2, p1, true);
    }
  }


  clean_up(lg1_write1, t1, lg2_write1, t2, buffer_manager);

  delete lg1_write; delete lg2_write; delete lg1_write1; delete lg2_write1;
}


// TEST(DeadlockTest, UpgradeWriteDeadlock){
//   BufferManager buffer_manager(128, 10);
//   uint64_t p1 = 1;
//   uint64_t t1 = 1, t2 = 2;
 
//   LockGrabber* lg1_read = new LockGrabber(buffer_manager, t1, p1, false);
//   LockGrabber* lg2_read = new LockGrabber(buffer_manager, t2, p1, false);

//   sleep(TIMEOUT);


//   // deadlock
//   LockGrabber* lg1_write = new LockGrabber(buffer_manager, t1, p1, true); 
//   LockGrabber* lg2_write = new LockGrabber(buffer_manager, t2, p1, true);


//   while(1){
//     EXPECT_FALSE(lg1_write->is_acquired() && lg2_write->is_acquired());
//     if(lg1_write->is_acquired() && !lg2_write->is_acquired()) break;
//     if(!lg1_write->is_acquired() && lg2_write->is_acquired()) break;

//     if(lg1_write->error()){
//       buffer_manager.transaction_abort(t1);
// 	  delete lg1_read; delete lg1_write;
//       sleep(WAIT_INTERVAL);
//       lg1_read = new LockGrabber(buffer_manager, t1, p1, false);
//       lg1_write = new LockGrabber(buffer_manager, t1, p1, true);
//     }

//     if(lg2_write->error()){
//       buffer_manager.transaction_abort(t2);
// 	  delete lg2_read; delete lg2_write;
//       sleep(WAIT_INTERVAL);
//       lg2_read = new LockGrabber(buffer_manager, t2, p1, false);
//       lg2_write = new LockGrabber(buffer_manager, t2, p1, true);
//     }
//   }

//   clean_up(lg1_write, t1, lg2_write, t2, buffer_manager);

//   delete lg1_read; delete lg2_read; delete lg1_write; delete lg2_write;
// }


}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  //testing::GTEST_FLAG(filter) = "DeadlockTest.UpgradeWriteDeadlock";
  return RUN_ALL_TESTS();
}
