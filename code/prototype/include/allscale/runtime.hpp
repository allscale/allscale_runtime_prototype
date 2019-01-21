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
		auto net = com::Network::create();
		Runtime rt(*net);

		// process the entry point on root node (rank 0)
		auto treeture = rt.getNetwork().runOn(0,[&](com::Node& node){

			std::mutex sync;
			sync.lock();

			work::treeture<int> treeture;

			// register a termination event handler
			allscale::utils::fiber::FiberEvents handler;
			handler.terminate = { [](void* l) { reinterpret_cast<std::mutex*>(l)->unlock(); } , &sync };

			node.getFiberContext().start([&]{

				// TODO:
				//  - create task ID
				//  - create task linked to treeture
				//  - return treeture

				using task_type = work::WorkItemTask<MainWorkItem,decltype(std::make_tuple(argc,argv))>;

				// create the task
				auto task = work::make_task<task_type>(0,std::make_tuple(argc,argv));

				// extract treeture
				treeture = task->getTreeture();

				// schedule task
				node.getService<work::WorkerPool>().schedule(std::move(task));

				// wait for completion
				treeture.wait();

			}, allscale::utils::fiber::Priority::MEDIUM, handler);

			sync.lock();

			// done
			return treeture;
		});

		// the rest should only run on node 0, the one with the valid treeture
		if (!treeture.valid()) {
			rt.shutdown();
			return 0;
		}

		// wait for completion
		auto res = std::move(treeture).get_result();

		std::cout << "Computation done, collecting statistics ...\n";

		// print statistics
		std::cout << "\n";
		rt.getNetwork().runOn(0,[](com::Node& node){
			node.getFiberContext().process([&]{
					std::cout << node.getLocalService<work::WorkerPool>().getStatistics();
			});
		});

		std::cout << "\n";
		std::cout << rt.getNetwork().getStatistics();
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

	using dependency   = allscale::runtime::work::TaskDependency;
	using dependencies = allscale::runtime::work::TaskDependencies;

	// creates empty dependencies
	dependencies after() {
		return dependencies{};
	}

	// creates dependencies from a list of dependencies
	template<typename ... TaskRefs>
	typename std::enable_if<(sizeof...(TaskRefs) > 0), dependencies>::type
	after(TaskRefs&& ... task_refs) {
		// create list of task references
		return { dependency{ task_refs.getTaskReference() } ... };
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
	treeture<void> treeture_parallel(const dependencies& deps, treeture<void>&& a, treeture<void>&& b) {
		deps.wait();
		return allscale::runtime::work::treeture_parallel(std::move(a),std::move(b));
	}

	// the parallel treeture connector
	template<typename A, typename B, typename Comp>
	treeture<std::result_of_t<Comp(A,B)>> treeture_combine(const dependencies& deps, treeture<A>&& a, treeture<B>&& b, const Comp& comp) {
		deps.wait();
		return allscale::runtime::work::treeture_combine(std::move(a),std::move(b),comp);
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
	treeture<typename WorkItemDesc::result_type> spawn(const runtime::dependencies& deps, const runtime::work::TaskID& id, Args&& ... args) {

		// create task based on work item description
		using task_type = runtime::work::WorkItemTask<WorkItemDesc,decltype(std::make_tuple(std::forward<Args>(args)...))>;

		// create the task
		auto task = runtime::work::make_task<task_type>(id,runtime::dependencies(deps), std::make_tuple(std::forward<Args>(args)...));

		// extract treeture
		auto treeture = task->getTreeture();

		// schedule task
		runtime::work::schedule(std::move(task));

		// return treeture
		return std::move(treeture);
	}


	template<typename WorkItemDesc, typename ... Args>
	treeture<typename WorkItemDesc::result_type> spawn(const runtime::work::TaskID& id, Args&& ... args) {
		return spawn(runtime::dependencies(),id,std::forward<Args>(args)...);
	}


	template<typename WorkItemDesc, typename ... Args>
	treeture<typename WorkItemDesc::result_type> spawn_first_with_dependencies(const runtime::dependencies& deps, Args&& ... args) {
		// get a fresh ID
		runtime::work::TaskID newId = allscale::runtime::work::getFreshId();
		return spawn<WorkItemDesc,Args...>(deps,newId,std::forward<Args>(args)...);
	}

	template<typename WorkItemDesc, typename ... Args>
	treeture<typename WorkItemDesc::result_type> spawn_with_dependencies(const runtime::dependencies& deps, Args&& ... args) {
		// get the ID of the child
		auto newId = runtime::work::getNewChildId();
		return spawn<WorkItemDesc,Args...>(deps,newId,std::forward<Args>(args)...);

	}


} // end of namespace allscale




// --------- Macro definitions ---------
#define ALLSCALE_REGISTER_TREETURE_TYPE(X)
#define REGISTER_DATAITEMSERVER_DECLARATION(X)
#define REGISTER_DATAITEMSERVER(X)

