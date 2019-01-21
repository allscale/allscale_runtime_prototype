/*
 * Infrastructure to facilitate the addressing of tasks.
 *
 *  Created on: Jan 21, 2019
 *      Author: herbert
 */

#pragma once

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
		com::rank_t owner = 0;

	public:

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
			// we can safely assume that the task is owned by
			// the same node, which is usually the case. In case the
			// task is not owned, the parent task will be owned, which
			// upon completion will signal the completion of child tasks
			// as well.
			return { id.getRightChild(), owner };		// < TODO: figure out why left and right is switched in generated code!
		}

		TaskRef getRightChild() const {
			// see: getLeftChild()
			return { id.getLeftChild(), owner };		// < TODO: figure out why left and right is switched in generated code!
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
			return out << "TaskRef(" << ref.id << "@" << ref.owner << ")";
		}

	};

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
