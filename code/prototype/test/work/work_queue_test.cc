#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/work_queue.h"

namespace allscale {
namespace runtime {
namespace work {

	template<typename Op>
	void runOnNode(const Op& op) {
		// get some network of any size
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		installTreetureStateService(net);
		net.runOn(0,[&](com::Node&){op();});
	}


	TEST(WorkQueue, Creation) {
		// simply test the creation
		WorkQueue queue;
	}

	TEST(WorkQueue, EmptyQueue) {
		// simply test the creation
		WorkQueue queue;

		EXPECT_TRUE(queue.empty());
		EXPECT_EQ(0,queue.size());

		// the dequeue operators deliver null-pointers
		EXPECT_FALSE(queue.dequeueBack());
		EXPECT_FALSE(queue.dequeueFront());
	}

	TEST(WorkQueue, InAndOut) {

		runOnNode([]{

			// simply test the creation
			WorkQueue queue;

			// create a number of tasks
			TaskPtr t1 = make_lambda_task(TaskID(0),[](){});
			TaskPtr t2 = make_lambda_task(TaskID(2),[](){});

			// save references to tasks
			Task& tr1 = *t1.get();
			Task& tr2 = *t2.get();

			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());

			// simple in-out (front)

			// enqueue in the front
			queue.enqueueFront(std::move(t1));
			EXPECT_FALSE(queue.empty());
			EXPECT_EQ(1,queue.size());

			// remove task from front
			t1 = queue.dequeueFront();
			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());
			EXPECT_EQ(tr1.getId(),t1->getId());


			// simple in-out (back)

			// enqueue in the back
			queue.enqueueBack(std::move(t1));
			EXPECT_FALSE(queue.empty());
			EXPECT_EQ(1,queue.size());

			// remove task from front
			t1 = queue.dequeueBack();
			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());
			EXPECT_EQ(tr1.getId(),t1->getId());



			// in front, out back (queue behavior)

			// enqueue in the front
			queue.enqueueFront(std::move(t1));
			queue.enqueueFront(std::move(t2));
			EXPECT_FALSE(queue.empty());
			EXPECT_EQ(2,queue.size());

			// remove task from back
			t1 = queue.dequeueBack();
			t2 = queue.dequeueBack();
			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());
			EXPECT_EQ(tr1.getId(),t1->getId());
			EXPECT_EQ(tr2.getId(),t2->getId());


			// in back, out front (queue behavior)

			// enqueue in the back
			queue.enqueueBack(std::move(t1));
			queue.enqueueBack(std::move(t2));
			EXPECT_FALSE(queue.empty());
			EXPECT_EQ(2,queue.size());

			// remove task from front
			t1 = queue.dequeueFront();
			t2 = queue.dequeueFront();
			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());
			EXPECT_EQ(tr1.getId(),t1->getId());
			EXPECT_EQ(tr2.getId(),t2->getId());


			// in front, out front (stack behavior)

			// enqueue in the front
			queue.enqueueFront(std::move(t1));
			queue.enqueueFront(std::move(t2));
			EXPECT_FALSE(queue.empty());
			EXPECT_EQ(2,queue.size());

			// remove task from front
			t2 = queue.dequeueFront();
			t1 = queue.dequeueFront();
			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());
			EXPECT_EQ(tr1.getId(),t1->getId());
			EXPECT_EQ(tr2.getId(),t2->getId());



			// in back, out back (stack behavior)

			// enqueue in the back
			queue.enqueueBack(std::move(t1));
			queue.enqueueBack(std::move(t2));
			EXPECT_FALSE(queue.empty());
			EXPECT_EQ(2,queue.size());

			// remove task from back
			t2 = queue.dequeueBack();
			t1 = queue.dequeueBack();
			EXPECT_TRUE(queue.empty());
			EXPECT_EQ(0,queue.size());
			EXPECT_EQ(tr1.getId(),t1->getId());
			EXPECT_EQ(tr2.getId(),t2->getId());



			// process tasks (to not destroy them non-processed)
			tr1.process();
			tr2.process();

		});
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
