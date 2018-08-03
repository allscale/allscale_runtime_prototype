/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <vector>

#include "allscale/utils/serializer.h"
#include "allscale/runtime/work/task_id.h"

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

	class DecisionTree {

		// the encoded form of the decision tree
		// Internally, it is stored in the form of an embedded tree,
		// each node represented by two bits; the two bits are the encoding
		// of the scheduling decision
		std::vector<std::uint8_t> encoded;

		DecisionTree(std::vector<std::uint8_t>&& data) : encoded(data) {}

	public:

		DecisionTree(int numNodes) : encoded(2*2*numNodes/8) {}	// 2 bits for 2x the number of nodes

		// updates a decision for a given path
		void set(const TaskPath& path, Decision decision);

		// retrieves the decision for a given path
		Decision get(const TaskPath& path) const;

		// provide a printer for debugging
		friend std::ostream& operator<<(std::ostream& out, const DecisionTree& tree);


		// --- serialization support ---

		static DecisionTree load(allscale::utils::ArchiveReader&);

		void store(allscale::utils::ArchiveWriter&) const;

	};


	/**
	 * A scheduling policy assisting the scheduler in deciding
	 * the direction in which to schedule tasks.
	 */
	class SchedulingPolicy {

		DecisionTree tree;

		SchedulingPolicy(DecisionTree&& data) : tree(std::move(data)) {}

	public:

		SchedulingPolicy() = delete;

		// --- factories ---

		/**
		 * Creates a scheduling policy distributing work on the given scheduling granularity
		 * level evenly (as close as possible) among the N available nodes.
		 *
		 * @param N the number of nodes to distribute work on
		 * @param granularity the negative exponent of the acceptable load imbalance; e.g. 0 => 2^0 = 100%, 3 => 2^-3 = 12.5%
		 */
		static SchedulingPolicy createUniform(int N, int granularity = 3);

		// create a balanced work distribution based on the given load distribution
		static SchedulingPolicy createReBalanced(const SchedulingPolicy& old, const std::vector<float>& loadDistribution);

		// --- observer ---

		const DecisionTree& getDecisionTree() const {
			return tree;
		}

		// --- the main interface for the scheduler ---

		Decision decide(const TaskPath& path) const {
			return tree.get(path);
		}

		// --- serialization support ---

		static SchedulingPolicy load(allscale::utils::ArchiveReader&);

		void store(allscale::utils::ArchiveWriter&) const;

		// --- printing ---

		// provide a printer for debugging
		friend std::ostream& operator<<(std::ostream& out, const SchedulingPolicy& p) {
			return out << p.tree;
		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
