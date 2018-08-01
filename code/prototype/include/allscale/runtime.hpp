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

#include "allscale/runtime/data/data_item_reference.h"
#include "allscale/runtime/data/data_item_requirement.h"
#include "allscale/runtime/data/data_item_manager.h"

#include "allscale/runtime/work/scheduler.h"
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
		auto net = com::Network::create();
		Runtime rt(*net);

		// process the entry point on root node (rank 0)
		auto treeture = rt.getNetwork().runOn(0,[&](com::Node& node){

			// TODO:
			//  - create task ID
			//  - create task linked to treeture
			//  - return treeture

			using task_type = work::WorkItemTask<MainWorkItem,decltype(std::make_tuple(argc,argv))>;

			// create the task
			auto task = work::make_task<task_type>(0,std::make_tuple(argc,argv));

			// extract treeture
			auto treeture = task->getTreeture();

			// schedule task
			node.getService<work::Worker>().schedule(std::move(task));

			// wait for completion
			treeture.wait();

			// done
			return std::move(treeture);
		});

		// wait for completion
		auto res = std::move(treeture).get_result();

		std::cout << "Comutation done, collecting statistics ...\n";

		// print statistics
		std::cout << "\n";
		std::cout << "------------------------------------------------------------------------------------------\n";
		std::cout << "Task execution statistics:\n";
		std::cout << "------------------------------------------------------------------------------------------\n";
		std::cout << "rank, split_tasks, processed_tasks\n";
		rt.getNetwork().runOnAll([](com::Node& node){
			std::cout << std::setw( 4) << node.getRank() << ",";
			std::cout << std::setw(12) << node.getService<work::Worker>().getNumSplitTasks() << ",";
			std::cout << std::setw(16) << node.getService<work::Worker>().getNumProcessedTasks() << "\n";

		});
		std::cout << "------------------------------------------------------------------------------------------\n";

		std::cout << "\n";
		std::cout << "------------------------------------------------------------------------------------------\n";
		std::cout << "Communication statistics:\n";
		std::cout << "------------------------------------------------------------------------------------------\n";
		std::cout << rt.getNetwork().getStatistics();
		std::cout << "------------------------------------------------------------------------------------------\n";
		std::cout << "\n";

		// shutdown network
		std::cout << "Shutting down runtime ...\n";
		rt.shutdown();

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
	using allscale::runtime::work::no_serialization;

	// a marker for serializable work items
	using allscale::runtime::work::do_serialization;

	// an empty type for unused types
	struct unused_type {};

	// the treeture class
	template<typename R>
	using treeture = allscale::runtime::work::treeture<R>;

	inline treeture<void> make_ready_treeture() {
		return treeture<void>(true);
	}

namespace runtime {

	// ---- utilities ----

	// special marker for returning values that are not used
	struct unused_type {};

	// ---- dependencies ----

	/**
	 * A class to model task dependencies.
	 */
	struct dependencies {
		/* not implemented yet */
	};

	// creates empty dependencies
	dependencies after() {
		return dependencies{};
	}

	// creates dependencies from a list of dependencies
	template<typename ... TaskRefs>
	typename std::enable_if<(sizeof...(TaskRefs) > 0), dependencies>::type
	after(TaskRefs&& ... task_refs) {
		assert_not_implemented() << "Dependencies not yet supported in prototype!";
		return after();
	}

	// ---- a prec operation wrapper ----

	template<typename R, typename Closure, typename F>
	struct prec_operation
	{
		typedef typename std::decay<Closure>::type closure_type;
		typedef typename std::decay<F>::type f_type;

		closure_type closure;
		f_type impl;

		template <typename A>
		treeture<R> operator()(A&& a) {
			return (*this)(dependencies{}, std::forward<A>(a));
		}

		template <typename A>
		treeture<R> operator()(dependencies const& d, A&& a) {
			return treeture<R>(impl(d,std::tuple_cat(std::make_tuple(std::forward<A>(a)),std::move(closure))));
		}

		template <typename A>
		treeture<R> operator()(dependencies && d, A&& a) {
			return treeture<R>(impl(std::move(d),std::tuple_cat(std::make_tuple(std::forward<A>(a)),std::move(closure))));
		}

	};

	template<typename A, typename R, typename Closure, typename F>
	prec_operation<R,Closure, F> make_prec_operation(Closure && closure, F&& impl)
	{
		return prec_operation<R, Closure, F>{
			std::forward<Closure>(closure),std::forward<F>(impl)};
	}

	// the parallel treeture connector
	treeture<void> treeture_parallel(const dependencies& /* ignored */, treeture<void>&& a, treeture<void>&& b) {
		return allscale::runtime::work::treeture_parallel(std::move(a),std::move(b));
	}

	// ---- Data Items ----

	// import data item reference into this namespace
	template<typename DataItemType>
	using DataItemReference = data::DataItemReference<DataItemType>;

	// import access modes
	using allscale::runtime::data::AccessMode;

	// import data item requirements into this namespace
	template<typename DataItemType>
	using DataItemRequirement = data::DataItemRequirement<DataItemType>;

	// import requirement factory
	using allscale::runtime::data::createDataItemRequirement;

	// also import other operations
	using allscale::runtime::data::DataItemManager;

} // end of namespace runtime

	template<typename WorkItemDesc, typename ... Args>
	treeture<typename WorkItemDesc::result_type> spawn(const runtime::work::TaskID& id, Args&& ... args) {

		// create task based on work item description
		using task_type = runtime::work::WorkItemTask<WorkItemDesc,decltype(std::make_tuple(std::forward<Args>(args)...))>;

		// create the task
		auto task = runtime::work::make_task<task_type>(id,std::make_tuple(std::forward<Args>(args)...));

		// extract treeture
		auto treeture = task->getTreeture();

		// schedule task
		runtime::work::schedule(std::move(task));

		// return treeture
		return std::move(treeture);
	}


	template<typename WorkItemDesc, typename ... Args>
	treeture<typename WorkItemDesc::result_type> spawn_first_with_dependencies(const runtime::dependencies& /* ignored */, Args&& ... args) {
		// get a fresh ID
		runtime::work::TaskID newId = allscale::runtime::work::getFreshId();
		return spawn<WorkItemDesc,Args...>(newId,std::forward<Args>(args)...);
	}

	template<typename WorkItemDesc, typename ... Args>
	treeture<typename WorkItemDesc::result_type> spawn_with_dependencies(const runtime::dependencies& /* ignored */, Args&& ... args) {
		// get the ID of the child
		auto newId = runtime::work::getNewChildId();
		return spawn<WorkItemDesc,Args...>(newId,std::forward<Args>(args)...);

	}


} // end of namespace allscale




// --------- Macro definitions ---------
#define ALLSCALE_REGISTER_TREETURE_TYPE(X)
#define REGISTER_DATAITEMSERVER_DECLARATION(X)
#define REGISTER_DATAITEMSERVER(X)

