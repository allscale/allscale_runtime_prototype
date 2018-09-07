#pragma once

#include "allscale/utils/serializer.h"

namespace allscale {
namespace utils {

	/**
	 * A common base type for scalar values, providing basic support
	 * for a range of comparison and arithmetic operators.
	 */
	template<typename T, typename Derived>
	class Scalar : public utils::trivially_serializable_if_t<T> {

		T value;

	public:

		// constructors

		Scalar() = default;
		Scalar(const T& val) : value(val) {}


		// -- operators --

		// - adding -

		Derived& operator+=(const Derived& other) {
			value += other.value;
			return asDerived();
		}

		Derived& operator+(const Derived& other) {
			return value + other.value;
		}

		template<typename V>
		Derived& operator+=(const V& v) {
			value += v;
			return asDerived();
		}

		template<typename V>
		Derived operator+(const V& v) const {
			return value + v;
		}

		Derived& operator-=(const Derived& other) {
			value += other.value;
			return asDerived();
		}

		Derived operator-(const Derived& other) {
			return value + other.value;
		}

		template<typename V>
		Derived& operator-=(const V& v) {
			value -= v;
			return asDerived();
		}

		template<typename V>
		Derived operator-(const V& v) const {
			return value - v;
		}


		// - scaling -

		template<typename S>
		Derived& operator*=(const S& s) {
			value *= s;
			return asDerived();
		}

		template<typename S>
		Derived operator*(const S& s) const {
			return value * s;
		}

		template<typename S>
		Derived& operator/=(const S& s) {
			value /= s;
			return asDerived();
		}

		template<typename S>
		Derived operator/(const S& s) const {
			return value / s;
		}

		double operator/(const Derived& other) const {
			return double(value) / double(other.value);
		}

		// - relations -

		bool operator<(const Derived& other) const {
			return value < other.value;
		}

		bool operator<=(const Derived& other) const {
			return value <= other.value;
		}

		bool operator==(const Derived& other) const {
			return value == other.value;
		}

		bool operator!=(const Derived& other) const {
			return value != other.value;
		}

		bool operator>=(const Derived& other) const {
			return value >= other.value;
		}

		bool operator>(const Derived& other) const {
			return value > other.value;
		}

		// -- accessors --

		T getValue() const {
			return value;
		}

	private:

		Derived& asDerived() {
			return static_cast<Derived&>(*this);
		}

		const Derived& asDerived() const {
			return static_cast<const Derived&>(*this);
		}

	};

	template<typename T, typename Derived, typename V>
	Derived operator+(const V& a,const Scalar<T,Derived>& b) {
		return b + a;
	}

	template<typename T, typename Derived, typename S>
	Derived operator*(const S& a,const Scalar<T,Derived>& b) {
		return b * a;
	}

} // end namespace utils
} // end namespace allscale
