/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2018 RapidMiner GmbH
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see 
 * https://www.gnu.org/licenses/.
 */

package com.rapidminer.belt.function;

import java.util.Objects;
import java.util.function.DoublePredicate;


/**
 * Predicate that tests a pair of double values. Derived from {@link java.util.function.DoublePredicate}.
 *
 * @author Michael Knopf
 */
@FunctionalInterface
public interface DoubleBinaryPredicate {

	/**
	 * Evaluates this predicate on the given arguments.
	 *
	 * @param first
	 * 		the first argument
	 * @param second
	 * 		the second argument
	 * @return {@code true} iff the input argument match the predicate
	 */
	boolean test(double first, double second);

	/**
	 * Returns a composed predicate that represents a logical AND of this and the given predicate (see {@link
	 * java.util.function.DoublePredicate#and(DoublePredicate)}).
	 *
	 * @param other
	 * 		the predicate to add to the composition
	 * @return the composed predicate
	 * @throws NullPointerException
	 * 		if other is {@code null}
	 */
	default DoubleBinaryPredicate and(DoubleBinaryPredicate other) {
		Objects.requireNonNull(other);
		return (a, b) -> test(a, b) && other.test(a, b);
	}

	/**
	 * Negates this predicate.
	 *
	 * @return the negated predicate
	 */
	default DoubleBinaryPredicate negate() {
		return (a, b) -> !test(a, b);
	}

	/**
	 * Returns a composed predicate that represents a logical OR of this and the given predicate (see {@link
	 * java.util.function.DoublePredicate#or(DoublePredicate)}).
	 *
	 * @param other
	 * 		the predicate to add to the composition
	 * @return the composed predicate
	 * @throws NullPointerException
	 * 		if other is {@code null}
	 */
	default DoubleBinaryPredicate or(DoubleBinaryPredicate other) {
		Objects.requireNonNull(other);
		return (a, b) -> test(a, b) || other.test(a, b);
	}

}
