#pragma once

#include <vector>
#include <ostream>

#include "allscale/utils/serializer.h"
#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace work {


	/**
	 * A utility to model sub-sets of nodes.
	 */
	class NodeMask {

		std::vector<bool> mask;

		NodeMask(std::vector<bool>&& mask) : mask(std::move(mask)) {}

	public:

		NodeMask(com::rank_t numNodes) : mask(numNodes,true) {}

		bool isActive(com::rank_t i) const {
			return i < mask.size() && mask[i];
		}

		void toggle(com::rank_t i) {
			mask[i] = !mask[i];
		}

		auto operator[](com::rank_t i) {
			return mask[i];
		}

		auto operator[](com::rank_t i) const {
			return mask[i];
		}

		/**
		 * Count the number of selected nodes.
		 */
		std::size_t count() const {
			std::size_t res = 0;
			for(bool cur : mask) {
				if (cur) res++;
			}
			return res;
		}

		/**
		 * Obtains the maximum number of nodes that could be included.
		 */
		std::size_t totalNodes() const {
			return mask.size();
		}

		NodeMask& addNode() {
			for(std::size_t i=0; i<mask.size(); i++) {
				if (!mask[i]) {
					mask[i] = true;
					return *this;
				}
			}
			return *this;
		}

		NodeMask& removeLast() {
			auto size = mask.size();
			for(std::size_t i=0; i<size; i++) {
				if (mask[size-i-1]) {
					mask[size-i-1]=false;
					return *this;
				}
			}
			return *this;
		}

		std::vector<com::rank_t> getNodes() const {
			std::vector<com::rank_t> res;
			res.reserve(totalNodes());
			for(com::rank_t i=0; i<totalNodes(); i++) {
				if (mask[i]) res.push_back(i);
			}
			return res;
		}

		const std::vector<bool>& asBitMask() const {
			return mask;
		}

		// serialization support

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write<std::size_t>(mask.size());
			for(std::size_t i=0; i<mask.size(); i++) {
				out.write<bool>(mask[i]);
			}
		}

		static NodeMask load(allscale::utils::ArchiveReader& in) {
			auto size = in.read<std::size_t>();
			std::vector<bool> res(size);
			for(std::size_t i=0; i<size; i++) {
				res[i] = in.read<bool>();
			}
			return std::move(res);
		}

		// -- printer support --

		friend std::ostream& operator<<(std::ostream& out, const NodeMask& mask) {
			std::vector<com::rank_t> list;
			for(std::size_t i=0; i<mask.mask.size(); i++) {
				if (mask.mask[i]) list.push_back(i);
			}
			return out << "NodeMask" << list;
		}
	};

} // end namespace work
} // end namespace runtime
} // end namespace allscale

