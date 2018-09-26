
#include "allscale/runtime/mon/task_stats.h"

#include <chrono>
#include <mutex>

#include "allscale/utils/assert.h"
#include "allscale/utils/printer/join.h"

namespace allscale {
namespace runtime {
namespace mon {

	constexpr int TRACKING_LEVEL = 6;


	TaskTimes::TaskTimes() : times(1<<TRACKING_LEVEL) {}


	void TaskTimes::add(const work::TaskID& id, const time_t& t) {
		add(id.getPath(),t);
	}

	void TaskTimes::add(const work::TaskPath& path, const time_t& t) {

		// on the right level, just keep a record
		if (path.getLength() == TRACKING_LEVEL) {
			times[path.getPath()] += t;
			return;
		}

		// if too coarse grained => split
		if (path.getLength() < TRACKING_LEVEL) {
			auto f = t/2;
			add(path.getLeftChildPath(),f);
			add(path.getRightChildPath(),t-f);
			return;
		}

		// if too fine, coarsen
		add(path.getParentPath(),t);
	}


	// -- observers --

	time_t TaskTimes::getTime(const work::TaskPath& path) const {

		// on the right level, return the recorded time
		if (path.getLength() == TRACKING_LEVEL) {
			return times[path.getPath()];
		}

		// on coarser levels, sum up partial times
		if (path.getLength() < TRACKING_LEVEL) {
			return getTime(path.getLeftChildPath()) + getTime(path.getRightChildPath());
		}

		// for too fine tasks, estimate half of parent time
		return getTime(path.getParentPath())/2;
	}


	// -- arithmetic --

	TaskTimes& TaskTimes::operator+=(const TaskTimes& other) {
		for(std::size_t i=0; i<times.size(); i++) {
			times[i] += other.times[i];
		}
		return *this;
	}

	TaskTimes TaskTimes::operator-(const TaskTimes& other) const {
		TaskTimes res = *this;
		for(std::size_t i=0; i<times.size(); i++) {
			res.times[i] -= other.times[i];
		}
		return res;
	}

	TaskTimes TaskTimes::operator/(float f) const {
		TaskTimes res = *this;
		for(auto& cur : res.times) {
			cur /= f;
		}
		return res;
	}


	// -- serialization --

	void TaskTimes::store(allscale::utils::ArchiveWriter& out) const {
		for(const auto& cur : times) {
			out.write(cur.count());
		}
	}

	TaskTimes TaskTimes::load(allscale::utils::ArchiveReader& in) {
		TaskTimes res;
		for(std::size_t i=0; i<res.times.size(); i++) {
			res.times[i] = time_t(in.read<time_t::rep>());
		}
		return res;
	}

	// -- printing --

	std::ostream& operator<<(std::ostream& out, const TaskTimes& times) {
		return out << "[" << allscale::utils::join(",",times.times,[](std::ostream& out, const auto& cur) {
			out << cur.count() << "ns";
		}) << "]";
	}


} // end namespace log
} // end namespace runtime
} // end namespace allscale
