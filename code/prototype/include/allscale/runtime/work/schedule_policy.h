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
		Stay =  0,		// < stay on current virtual node
		Left =  1, 		// < send to left child
		Right = 2		// < send to right child
	};


	std::ostream& operator<<(std::ostream&,Decision);

	class DecisionTree {

		// the encoded form of the decision tree
		// Internally, it is stored in the form of an embedded tree,
		// each node represented by two bits; the two bits are the encoding
		// of the scheduling decision
		std::vector<bool> encoded;

	public:

		DecisionTree(int numNodes) : encoded(2*2*numNodes) {}	// 2 bits for 2x the number of nodes

		// updates a decision for a given path
		void set(const TaskPath& path, Decision decision);

		// retrieves the decision for a given path
		Decision get(const TaskPath& path) const;

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

		// create a uniform distributing policy for N nodes
		static SchedulingPolicy createUniform(int N, int extraDepth = 3);

		// create a balanced work distribution based on the given load distribution
		static SchedulingPolicy createBalanced(const std::vector<float>& loadDistribution, int extraDepth = 3);


		// --- the main interface for the scheduler ---

		Decision decide(const TaskPath& path) const {
			return tree.get(path);
		}

		// --- serialization support ---

		static SchedulingPolicy load(allscale::utils::ArchiveReader&);

		void store(allscale::utils::ArchiveWriter&) const;

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
