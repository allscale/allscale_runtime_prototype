/*
 * Infrastructure to facilitate the addressing of tasks.
 *
 *  Created on: Jan 21, 2019
 *      Author: herbert
 */

#pragma once

#include <limits>
#include <allscale/utils/serializer.h>

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/work/task_id.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * A reference to a task in the system, freely copy-, move-, and serializable.
	 */
	class TaskRef : public allscale::utils::trivially_serializable {

		// the ID of the task this treeture is associated to
		TaskID id;

		// the rank the associated treeture state is maintained by
		com::rank_t owner;

	public:

		// the owner assigned if the owner is unknown
		constexpr static com::rank_t UNKNOWN_OWNER = std::numeric_limits<com::rank_t>::max();

		TaskRef() = default;

		TaskRef(const TaskID& id, com::rank_t owner)
			: id(id), owner(owner) {}

		TaskRef(const TaskRef&) = default;
		TaskRef(TaskRef&&) = default;

		// -- operator --

		TaskRef& operator=(const TaskRef&) = default;
		TaskRef& operator=(TaskRef&&) = default;

		// -- factories --

		TaskRef getLeftChild() const {
			// we don't know for certain the owner of the child task
			return { id.getLeftChild(), UNKNOWN_OWNER };
		}

		TaskRef getRightChild() const {
			// see: getLeftChild()
			return { id.getRightChild(), UNKNOWN_OWNER };
		}

		// -- observer --

		com::rank_t getOwner() const {
			return owner;
		}

		const TaskID& getTaskID() const {
			return id;
		}

		// add printer support
		friend std::ostream& operator<<(std::ostream& out, const TaskRef& ref) {
			out << "TaskRef(" << ref.id << "@";
			if (ref.owner == UNKNOWN_OWNER) {
				out << "?";
			} else {
				out << ref.owner;
			}
			return out << ")";
		}

	};

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
