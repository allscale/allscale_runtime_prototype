/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <memory>
#include <random>
#include <vector>

#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/functions.h"

#include "allscale/runtime/com/hierarchy.h"
#include "allscale/runtime/work/node_mask.h"
#include "allscale/runtime/work/task_id.h"
#include "allscale/runtime/mon/task_stats.h"

namespace allscale {
namespace runtime {
namespace work {


	// ---------------------------------------------------------------
	//					   Scheduler Policy
	// ---------------------------------------------------------------


	enum class Decision {
		Done =  0,		// < this task has reached its destination
		Stay =  1,		// < stay on current virtual node
		Left =  2, 		// < send to left child
		Right = 3		// < send to right child
	};


	std::ostream& operator<<(std::ostream&,Decision);



	/**
	 * An abstract base class for scheduling policies.
	 */
	class SchedulingPolicy {

		using load_fun = std::unique_ptr<SchedulingPolicy>(*)(allscale::utils::ArchiveReader&);

		load_fun loader;

	public:

		SchedulingPolicy(const load_fun& loader) : loader(loader) {};

		// a virtual destructor for derived types
		virtual ~SchedulingPolicy() {}

		/**
		 * Determines whether the node with the given address is part of the dispatching of a task with the given path.
		 *
		 * @param addr the address in the hierarchy to be tested
		 * @param path the path to be tested
		 */
		virtual bool isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const =0;

		/**
		 * Obtains the scheduling decision at the given node. The given node must be involved in
		 * the scheduling of the given path.
		 */
		virtual Decision decide(const com::HierarchyAddress& addr, const TaskPath& path) const =0;

		/**
		 * Tests whether the given target address is a a valid target for the given path.
		 * This is utilized for debugging.
		 */
		virtual bool checkTarget(const com::HierarchyAddress& addr, const TaskPath& path) const =0;

		// --- cloning ---

		virtual std::unique_ptr<SchedulingPolicy> clone() const =0;

		// --- printing support ---

		// requests print support for all implementations
		virtual void printTo(std::ostream& out) const =0;

		friend std::ostream& operator<<(std::ostream& out, const SchedulingPolicy& policy);

		// --- serialization support ---

		static std::unique_ptr<SchedulingPolicy> load(allscale::utils::ArchiveReader& in) {
			return in.read<load_fun>()(in);
		}

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(loader);
			storeInternal(out);
		}

		virtual void storeInternal(allscale::utils::ArchiveWriter&) const =0;

	};


	/**
	 * A modifyable scheduling policy, also supporting direct serialization.
	 */
	class ExchangeableSchedulingPolicy : public SchedulingPolicy {

		// the policy currently represented
		std::unique_ptr<SchedulingPolicy> policy;

	public:

		// DO NOT USE, should be private, but std::make_unique requires access.
		ExchangeableSchedulingPolicy(std::unique_ptr<SchedulingPolicy>&& policy);

		ExchangeableSchedulingPolicy(const SchedulingPolicy& policy);

		ExchangeableSchedulingPolicy(const ExchangeableSchedulingPolicy& other);
		ExchangeableSchedulingPolicy(ExchangeableSchedulingPolicy&&) = default;

		ExchangeableSchedulingPolicy& operator=(const ExchangeableSchedulingPolicy&);
		ExchangeableSchedulingPolicy& operator=(ExchangeableSchedulingPolicy&&) = default;


		// --- observers / mutators ---

		template<typename T>
		bool isa() const {
			return dynamic_cast<T*>(policy.get());
		}

		const SchedulingPolicy& getPolicy() const {
			return *policy;
		}

		SchedulingPolicy& getPolicy() {
			return *policy;
		}

		template<typename T>
		const T& getPolicy() const {
			assert_true(isa<T>());
			return static_cast<const T&>(*policy);
		}

		template<typename T>
		T& getPolicy() {
			assert_true(isa<T>());
			return static_cast<T&>(*policy);
		}

		template<typename P, typename ... Args>
		void setPolicy(Args&& ... args) {
			policy = std::make_unique<P>(std::forward<Args>(args)...);
		}

		void setPolicy(std::unique_ptr<SchedulingPolicy>&& newPolicy) {
			assert_true(bool(newPolicy));
			policy = std::move(newPolicy);
		}


		// --- scheduling interface ---

		bool isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const override {
			return policy->isInvolved(addr,path);
		}

		Decision decide(const com::HierarchyAddress& addr, const TaskPath& path) const override {
			return policy->decide(addr,path);
		}

		bool checkTarget(const com::HierarchyAddress& addr, const TaskPath& path) const override {
			return policy->checkTarget(addr,path);
		}


		// --- cloning ---

		std::unique_ptr<SchedulingPolicy> clone() const override;


		// --- printing support ---

		void printTo(std::ostream& out) const override {
			policy->printTo(out);
		}


		// --- serialization support ---

		static ExchangeableSchedulingPolicy load(allscale::utils::ArchiveReader& in) {
			return SchedulingPolicy::load(in);
		}

		void store(allscale::utils::ArchiveWriter& out) const {
			policy->store(out);
		}

		virtual void storeInternal(allscale::utils::ArchiveWriter& out) const override {
			policy->store(out);
		}

	private:

		static std::unique_ptr<SchedulingPolicy> loadAsUniquePtr(allscale::utils::ArchiveReader& in) {
			return std::make_unique<ExchangeableSchedulingPolicy>(load(in));
		}

	};



	// ---------------------------------------------------------------
	//				      Random Policy
	// ---------------------------------------------------------------

	/**
	 * A scheduling policy assisting the scheduler in deciding
	 * the direction in which to schedule tasks.
	 */
	class RandomSchedulingPolicy : public SchedulingPolicy {

		// the root node of the network to be scheduling for
		com::HierarchyAddress root;

		// the task-cut-off level (determining granulartiy of processed tasks)
		int cutOffLevel;

		// a random device - because only random is truly fair
		mutable std::uniform_real_distribution<> policy;

		// the source for random values
		mutable std::random_device generator;

	public:

		RandomSchedulingPolicy(const com::HierarchyAddress& root, int cutOffLevel)
			: SchedulingPolicy(loadAsUniquePtr), root(root), cutOffLevel(cutOffLevel) {}


		// --- the main interface for the scheduler ---

		/**
		 * Determines whether the node with the given address is part of the dispatching of a task with the given path.
		 *
		 * @param addr the address in the hierarchy to be tested
		 * @param path the path to be tested
		 */
		bool isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const override;

		/**
		 * Obtains the scheduling decision at the given node. The given node must be involved in
		 * the scheduling of the given path.
		 */
		Decision decide(const com::HierarchyAddress& addr, const TaskPath& path) const override;

		/**
		 * Tests whether the given address is a valid target for a task with the given task.
		 */
		bool checkTarget(const com::HierarchyAddress&, const TaskPath&) const override {
			return true; // every target is ok
		}

		// --- cloning ---

		std::unique_ptr<SchedulingPolicy> clone() const override;

		// --- printing support ---

		// requests print support for all implementations
		void printTo(std::ostream& out) const override;

		// --- serialization support ---

		void storeInternal(allscale::utils::ArchiveWriter& out) const override;

	private:

		static std::unique_ptr<SchedulingPolicy> loadAsUniquePtr(allscale::utils::ArchiveReader& in);

	};

	// ---------------------------------------------------------------
	//				 Decision Tree Based Policy
	// ---------------------------------------------------------------

	class DecisionTree {

		// the encoded form of the decision tree
		// Internally, it is stored in the form of an embedded tree,
		// each node represented by two bits; the two bits are the encoding
		// of the scheduling decision
		std::vector<std::uint8_t> encoded;

		DecisionTree(std::vector<std::uint8_t>&& data) : encoded(data) {}

	public:

		DecisionTree(std::uint64_t numNodes);

		// updates a decision for a given path
		void set(const TaskPath& path, Decision decision);

		// retrieves the decision for a given path
		Decision get(const TaskPath& path) const;

		// provide a printer for debugging
		friend std::ostream& operator<<(std::ostream& out, const DecisionTree& tree);

		// --- operators ---

		bool operator==(const DecisionTree& other) const {
			return encoded == other.encoded;
		}

		bool operator!=(const DecisionTree& other) const {
			return !(*this == other);
		}

		// --- serialization support ---

		static DecisionTree load(allscale::utils::ArchiveReader&);

		void store(allscale::utils::ArchiveWriter&) const;

	};


	/**
	 * A scheduling policy assisting the scheduler in deciding
	 * the direction in which to schedule tasks.
	 */
	class DecisionTreeSchedulingPolicy : public SchedulingPolicy {

		// the address of the root node of the network this policy is defined for
		com::HierarchyAddress root;

		// the balancing granularity this policy was created for
		int granulartiy;

		// the routing-decision tree
		DecisionTree tree;

	public:

		// Do not use directly, should be private, but is required by std::make_unique ...
		DecisionTreeSchedulingPolicy(com::HierarchyAddress root, int granulartiy, DecisionTree&& data)
			: SchedulingPolicy(loadAsUniquePtr), root(root), granulartiy(granulartiy), tree(std::move(data)) {}

		DecisionTreeSchedulingPolicy() = delete;

		// --- factories ---

		/**
		 * Creates a scheduling policy distributing work on the given scheduling granularity
		 * level evenly (as close as possible) among the nodes enabled in the given mask.
		 *
		 * @param a bit set of nodes to be included in the schedule (one bit must at least be set)
		 * @param granularity the negative exponent of the acceptable load imbalance; e.g. 0 => 2^0 = 100%, 5 => 2^-5 = 3.125%
		 */
		static DecisionTreeSchedulingPolicy createUniform(const NodeMask& mask, int granularity);

		/**
		 * Creates a scheduling policy distributing work uniformly among the given set of nodes. The
		 * granulartiy will be adjusted accordingly, such that ~8 tasks per node are created.
		 *
		 * @param a bit set of nodes to be included in the schedule (one bit must at least be set)
		 */
		static DecisionTreeSchedulingPolicy createUniform(const NodeMask& mask);

		/**
		 * Creates a scheduling policy distributing work on the given scheduling granularity
		 * level evenly (as close as possible) among the N available nodes.
		 *
		 * @param N the number of nodes to distribute work on
		 * @param granularity the negative exponent of the acceptable load imbalance; e.g. 0 => 2^0 = 100%, 5 => 2^-5 = 3.125%
		 */
		static DecisionTreeSchedulingPolicy createUniform(int N, int granularity);

		/**
		 * Creates a scheduling policy distributing work uniformly among the given number of nodes. The
		 * granulartiy will be adjusted accordingly, such that ~8 tasks per node are created.
		 *
		 * @param N the number of nodes to distribute work on
		 */
		static DecisionTreeSchedulingPolicy createUniform(int N);

		/**
		 * Creates an updated load balancing policy based on a given policy and a measured load distribution.
		 * The resulting policy will distributed load evenly among the available nodes, weighted by the observed load.
		 *
		 * @param old the old policy, based on which the measurement has been taken
		 * @param loadDistribution the load distribution measured, utilized for weighting tasks. There must be one entry per node,
		 * 			no entry must be negative.
		 */
		static DecisionTreeSchedulingPolicy createReBalanced(const DecisionTreeSchedulingPolicy& old, const std::vector<float>& loadDistribution);

		/**
		 * Creates an updated load balancing policy based on a given policy, a measured load distribution, and a mask of nodes to be involved.
		 * The resulting policy will distributed load evenly among the enabled nodes, weighted by the observed load.
		 *
		 * @param old the old policy, based on which the measurement has been taken
		 * @param loadDistribution the load distribution measured, utilized for weighting tasks. There must be one entry per node,
		 * 			no entry must be negative.
		 * @param mask a mask indicating which nodes to utilize and which not in the resulting schedule
		 */
		static DecisionTreeSchedulingPolicy createReBalanced(const DecisionTreeSchedulingPolicy& old, const std::vector<float>& loadDistribution, const NodeMask& mask);

		/**
		 * Creates an updated load balancing policy based on a given policy, a measured task time summary, and a mask of nodes to be involved.
		 * The resulting policy will distributed load evenly among the enabled nodes, weighted by the observed task times.
		 *
		 * @param old the old policy, based on which the measurement has been taken
		 * @param task times measured, utilized for re-assigning tasks.
		 * @param mask a mask indicating which nodes to utilize and which not in the resulting schedule
		 */
		static DecisionTreeSchedulingPolicy createReBalanced(const DecisionTreeSchedulingPolicy& old, const mon::TaskTimes& taskTimes, const NodeMask& mask);

		// --- observer ---

		const com::HierarchyAddress& getPresumedRootAddress() const {
			return root;
		}

		/**
		 * The scheduling granularity, thus the maximum depth of a task-path still routed
		 * by this scheduling policy.
		 */
		int getGranularity() const {
			return granulartiy;
		}

		const DecisionTree& getDecisionTree() const {
			return tree;
		}

		// retrieves the task distribution pattern this tree is realizing
		std::vector<com::rank_t> getTaskDistributionMapping() const;


		// --- the main interface for the scheduler ---

		/**
		 * Determines whether the node with the given address is part of the dispatching of a task with the given path.
		 *
		 * @param addr the address in the hierarchy to be tested
		 * @param path the path to be tested
		 */
		bool isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const override;

		/**
		 * Obtains the scheduling decision at the given node. The given node must be involved in
		 * the scheduling of the given path.
		 */
		Decision decide(const com::HierarchyAddress& addr, const TaskPath& path) const override;

		/**
		 * Tests whether the given address is a valid target for a task with the given task.
		 */
		bool checkTarget(const com::HierarchyAddress& addr, const TaskPath& path) const override {
			return addr == getTarget(path);
		}

		/**
		 * Computes the target address a task with the given path should be forwarded to.
		 */
		com::HierarchyAddress getTarget(const TaskPath& path) const;

		// --- operator ---

		bool operator==(const DecisionTreeSchedulingPolicy& other) const {
			return tree == other.tree;
		}

		bool operator!=(const DecisionTreeSchedulingPolicy& other) const {
			return tree != other.tree;
		}

		// --- cloning ---

		std::unique_ptr<SchedulingPolicy> clone() const override;


		// --- printing support ---

		void printTo(std::ostream& out) const override;


		// --- serialization support ---

		static std::unique_ptr<SchedulingPolicy> loadAsUniquePtr(allscale::utils::ArchiveReader&);

		void storeInternal(allscale::utils::ArchiveWriter&) const override;

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
