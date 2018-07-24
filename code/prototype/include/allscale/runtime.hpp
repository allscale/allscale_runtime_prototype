/*
 * The runtime interface of the prototype implementation.
 *
 * This interface is identical to the interface offered by the AllScale runtime, enabling
 * compiler generated applications to be processed within the prototype.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <cstdlib>
#include <iomanip>
#include <tuple>

#include "allscale/runtime/work/task.h"
#include "allscale/runtime/work/treeture.h"
#include "allscale/runtime/work/work_item.h"
#include "allscale/runtime/work/worker.h"

#include "allscale/runtime/runtime.h"

namespace allscale {
namespace runtime {


	/**
	 * The wrapper for the main function of an application handling the startup
	 * and shutdown procedure as well as lunching the entry point work item.
	 */
	template<typename MainWorkItem>
	int main_wrapper(int argc, char** argv) {

		// create the processing network
		std::cout << "Starting up runtime ...\n";
		Runtime rt = Runtime::create();

		// process the entry point on root node (rank 0)
		auto treeture = rt.getNetwork().runOn(0,[&](com::Node& node){

			// TODO:
			//  - create task ID
			//  - create task linked to treeture
			//  - return treeture

			using task_type = work::WorkItemTask<MainWorkItem>;

			// create the task
			auto task = work::make_task<task_type>(0,std::make_tuple(argc,argv));

			// extract treeture
			auto treeture = task->getTreeture();

			// schedule task
			node.getService<work::Worker>().schedule(std::move(task));

			// done
			return treeture;
		});

		// wait for completion
		auto res = std::move(treeture).get();

		// shutdown network
		std::cout << "Shutting down runtime ...\n";
		rt.shutdown();

		// print statistics
		std::cout << "\n";
		std::cout << "------------------------------------------------------------\n";
		std::cout << "Task execution statistics:\n";
		std::cout << "------------------------------------------------------------\n";
		std::cout << "rank, num_tasks\n";
		rt.getNetwork().runOnAll([](com::Node& node){
			std::cout << std::setw( 4) << node.getRank() << ",";
			std::cout << std::setw(10) << node.getService<work::Worker>().getNumProcessedTasks() << "\n";

		});
		std::cout << "------------------------------------------------------------\n";

		std::cout << "\n";
		std::cout << "------------------------------------------------------------\n";
		std::cout << "Communication statistics:\n";
		std::cout << "------------------------------------------------------------\n";
		std::cout << rt.getNetwork().getStatistics();
		std::cout << "------------------------------------------------------------\n";

		// return result
		return res;
	}


} // end of namespace runtime
} // end of namespace allscale


// --------- Interface adapter ---------

// the allscale runtime is "leaking" the std namespace include
namespace std {};
using namespace std;

// the allscale runtime also depends on hpx treetures

namespace hpx {
namespace util {

	// use std::tuple as hpx tuple replacement
	template<typename ... Args>
	using tuple = std::tuple<Args...>;

	// we also need to include the get function
	using std::get;

} // end namespace util
} // end namespace hpx


namespace allscale {

	// the work item descriptor class
	template <
		typename Result,				// the result type of this work item
		typename Name,					// a struct producing the name of this work item
		typename SerializationFlag,		// this one is ignored
		typename SplitVariant,			// the split variant implementation
		typename ProcessVariant,		// the process variant implementation
		typename CanSplitTest			// the can-split test
	>
	using work_item_description = allscale::runtime::work::work_item_description<Result,Name,SerializationFlag,SplitVariant,ProcessVariant,CanSplitTest>;

	// a marker for not serializable work items
	struct no_serialization {};

	// a marker for serializable work items
	struct do_serialization {};

	// an empty type for unused types
	struct unused_type {};

	// the treeture class
	template<typename R>
	using treeture = allscale::runtime::work::treeture<R>;

} // end of namespace allscale

// --------- Macro definitions ---------
#define ALLSCALE_REGISTER_TREETURE_TYPE(X)
