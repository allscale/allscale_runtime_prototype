/*
 * Some utilities to navigate in an hierarchical overlay-network.
 *
 *  Created on: Jul 26, 2018
 *      Author: herbert
 */

#pragma once

#include <ostream>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace com {


	// define the type utilized to address layers
	using layer_t = uint32_t;

	// defines the address of a virtual node in the hierarchy
	class HierarchyAddress : public allscale::utils::trivially_serializable {

		// the addressed rank
		rank_t rank;

		// the addressed layer
		layer_t layer;

	public:

		// creates a new address targeting the given rank on the given node
		HierarchyAddress(rank_t rank = 0, layer_t layer = 0) : rank(rank), layer(layer) {
			// assert that the provided combination is valid
			assert_true(check());
		}

		// --- observer ---

		// obtains the rank of the node the addressed virtual node is hosted
		rank_t getRank() const {
			return rank;
		}

		// obtains the layer on the hosting node this address is referencing
		layer_t getLayer() const {
			return layer;
		}

		// tests whether this address is referencing an actual node
		bool isLeaf() const {
			return layer == 0;
		}

		// tests whether this address is referencing a virtual management node
		bool isVirtualNode() const {
			return !isLeaf();
		}

		// determine whether this node is the left child of its parent
		bool isLeftChild() const {
			return !isRightChild();
		}

		// determines whether this node is the right child of its parent
		bool isRightChild() const {
			return rank & (1<<layer);
		}

		// --- factories ---

		/**
		 * Obtains the root node of a network of the given size.
		 */
		static HierarchyAddress getRootOfNetworkSize(rank_t size);

		// --- navigation ---

		/**
		 * Obtains the address of the parent node.
		 */
		HierarchyAddress getParent() const {
			// computes the address of the parent (always possible)
			return { rank & ~(1<<layer), layer+1 };
		}

		/**
		 * Obtains the address of an anchester node.
		 */
		HierarchyAddress getParent(int steps) const {
			if (steps == 0) return *this;
			return getParent().getParent(steps-1);
		}

		/**
		 * Obtains the address of the left child.
		 * ATTENTION: only supported for virtual nodes
		 */
		HierarchyAddress getLeftChild() const {
			// computes the address of the left child
			assert_true(isVirtualNode());
			return { rank, layer-1 };
		}

		/**
		 * Obtains the address of the left child.
		 * ATTENTION: only supported for virtual nodes
		 */
		HierarchyAddress getRightChild() const {
			// computes the address of the right child
			assert_true(isVirtualNode());
			return { rank+(1<<(layer-1)), layer-1 };
		}

		// --- operators ---

		bool operator==(const HierarchyAddress& other) const {
			return rank == other.rank && layer == other.layer;
		}

		bool operator!=(const HierarchyAddress& other) const {
			return !(*this == other);
		}

		bool operator<(const HierarchyAddress& other) const {
			return rank < other.rank || (rank == other.rank && layer < other.layer);
		}

		// add printer support
		friend std::ostream& operator<<(std::ostream&, const HierarchyAddress&);

		// --- utilities ---

		// computes the number of layers present on the given rank within a network of the given size
		static layer_t getLayersOn(rank_t rank, rank_t size);

	private:

		// tests whether the given rank/layer combination is valid
		bool check() const;

	};


	/**
	 * A wrapper for services transforming those into a hierarchical service.
	 * There will be one service instance per virtual node.
	 */
	template<typename Service>
	class HierarchyService {

		// the locally running services (one instance for each layer)
		std::vector<Service> services;

	public:

		// starts up a hierarchical service on this node
		template<typename ... Args>
		HierarchyService(Node& node, const Args& ... args) {
			// start up services
			auto numServices = HierarchyAddress::getLayersOn(node.getRank(),node.getNetwork().numNodes());
			services.reserve(numServices);
			for(layer_t i=0; i<numServices; i++) {
				services.emplace_back(node.getNetwork(), HierarchyAddress(node.getRank(), i), args...);
			}
		}

		// retrieves a service instance
		Service& get(layer_t layer) {
			assert_lt(layer,services.size());
			return services[layer];
		}

		// applies an operation on all local services
		template<typename Op>
		void forAll(const Op& op) {
			for(auto& cur : services) {
				op(cur);
			}
		}

	};

	/**
	 * A hierarchical overlay network that can be placed on top of an existing network.
	 */
	class HierarchicalOverlayNetwork {

		// the network build up on
		Network& network;

	public:

		// creates the hierarchical view on this network
		HierarchicalOverlayNetwork(Network& network)
			: network(network) {}

		/**
		 * Retrieves the number of nodes in this network.
		 */
		size_t numNodes() const {
			return network.numNodes();
		}

		/**
		 * Obtains the address of the root node of this overlay network.
		 */
		HierarchyAddress getRootAddress() const {
			return HierarchyAddress::getRootOfNetworkSize(network.numNodes());
		}

		/**
		 * Installs a hierarchical service on all virtual nodes.
		 */
		template<typename S, typename ... Args>
		void installServiceOnNodes(const Args& ... args) {
			network.installServiceOnNodes<HierarchyService<S>>(args...);
		}

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename S, typename R, typename ... Args>
		auto getRemoteProcedure(const HierarchyAddress& addr, R(S::*fun)(Args...)) {
			return network.getRemoteProcedure(addr.getRank(),[addr](Node& node)->S&{
				return node.getService<HierarchyService<S>>().get(addr.getLayer());
			},fun);
		}

		/**
		 * Obtains a reference to a locally running service instance.
		 */
		template<typename S>
		static S& getLocalService(layer_t layer = 0) {
			return com::Node::getLocalService<HierarchyService<S>>().get(layer);
		}

		/**
		 * Applies the given operation on all local service instances.
		 */
		template<typename S, typename Op>
		static void forAllLocal(const Op& op) {
			com::Node::getLocalService<HierarchyService<S>>().forAll(op);
		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
