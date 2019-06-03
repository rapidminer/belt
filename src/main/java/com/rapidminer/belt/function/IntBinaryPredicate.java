/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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
import java.util.function.IntPredicate;


/**
 * Predicate that tests a pair of int values. Derived from {@link IntPredicate}.
 *
 * @author Michael Knopf, Gisa Meier
 */
@FunctionalInterface
public interface IntBinaryPredicate {

	/**
	 * Evaluates this predicate on the given arguments.
	 *
	 * @param first
	 * 		the first argument
	 * @param second
	 * 		the second argument
	 * @return {@code true} iff the input argument match the predicate
	 */
	boolean test(int first, int second);

	/**
	 * Returns a composed predicate that represents a logical AND of this and the given predicate (see {@link
	 * IntPredicate#and(IntPredicate)}).
	 *
	 * @param other
	 * 		the predicate to add to the composition
	 * @return the composed predicate
	 * @throws NullPointerException
	 * 		if other is {@code null}
	 */
	default IntBinaryPredicate and(IntBinaryPredicate other) {
		Objects.requireNonNull(other);
		return (a, b) -> test(a, b) && other.test(a, b);
	}

	/**
	 * Negates this predicate.
	 *
	 * @return the negated predicate
	 */
	default IntBinaryPredicate negate() {
		return (a, b) -> !test(a, b);
	}

	/**
	 * Returns a composed predicate that represents a logical OR of this and the given predicate (see {@link
	 * IntPredicate#or(IntPredicate)}).
	 *
	 * @param other
	 * 		the predicate to add to the composition
	 * @return the composed predicate
	 * @throws NullPointerException
	 * 		if other is {@code null}
	 */
	default IntBinaryPredicate or(IntBinaryPredicate other) {
		Objects.requireNonNull(other);
		return (a, b) -> test(a, b) || other.test(a, b);
	}

}
