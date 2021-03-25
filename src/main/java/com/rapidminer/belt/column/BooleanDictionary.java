/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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

package com.rapidminer.belt.column;

import java.util.List;
import java.util.Objects;


/**
 * A {@link Dictionary} that is boolean (see {@link #isBoolean()}. It can have at most two values and knows a positive
 * and/or negative index.
 *
 * @author Gisa Meier
 */
public class BooleanDictionary extends Dictionary {

	/**
	 * The placeholder for the category index when there is no positive or negative index.
	 */
	public static final int NO_ENTRY = -1;

	/**
	 * The maximal size of the list underlying a boolean dictionary.
	 */
	public static final int MAXIMAL_RAW_SIZE = 3;

	private static final int MAXIMAL_INDEX = MAXIMAL_RAW_SIZE - 1;

	private final int positiveIndex;

	/**
	 * Creates a new boolean dictionary
	 *
	 * @param dictionary
	 * 		a list of the form (null), (null, a), (null, a, b) or (null, null, a)
	 * @param positiveIndex
	 * 		an existing index except 0 or {@link #NO_ENTRY} and in case of (null, a, b) it must be either 1 or 2
	 */
	BooleanDictionary(List<String> dictionary, int positiveIndex) {
		super(dictionary, (dictionary.size() > 1 && dictionary.get(1) == null) ? 1 : 0);
		this.positiveIndex = positiveIndex;
		sanityCheck(dictionary, positiveIndex);
	}

	/**
	 * Checks that the dictionary has a sensible positive value and a valid size.
	 */
	private static void sanityCheck(List<String> dictionary, int positiveIndex) {
		if (dictionary.size() > MAXIMAL_RAW_SIZE) {
			throw new IllegalArgumentException("Too many values for boolean");
		}
		if (positiveIndex == 0 || positiveIndex < NO_ENTRY || positiveIndex >= dictionary.size()
				|| (positiveIndex == NO_ENTRY && dictionary.size() == MAXIMAL_RAW_SIZE && dictionary.get(1) != null)
				|| (positiveIndex == 1 && dictionary.get(1) == null)) {
			throw new IllegalArgumentException("Invalid positive index");
		}
	}

	@Override
	public boolean isBoolean() {
		return true;
	}

	@Override
	public boolean hasPositive() {
		return positiveIndex != NO_ENTRY;
	}

	@Override
	public boolean hasNegative() {
		return (positiveIndex == MAXIMAL_INDEX && get(1) != null) || (positiveIndex < maximalIndex() && size() > 0);
	}

	@Override
	public int getPositiveIndex() {
		return positiveIndex;
	}

	@Override
	public int getNegativeIndex() {
		if (positiveIndex == MAXIMAL_INDEX && get(1) != null) {
			return 1;
		}
		if (size() == 0) {
			return NO_ENTRY;
		}
		if (positiveIndex < maximalIndex()) {
			return maximalIndex();
		}
		return NO_ENTRY;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		BooleanDictionary that = (BooleanDictionary) o;
		return positiveIndex == that.positiveIndex;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), positiveIndex);
	}

	@Override
	public String toString() {
		return super.toString() + (positiveIndex == NO_ENTRY ? ", no positive" : ", positive: " + get(positiveIndex));
	}
}
