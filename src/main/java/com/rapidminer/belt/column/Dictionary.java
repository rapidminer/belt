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

package com.rapidminer.belt.column;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.reader.CategoricalReader;


/**
 * A dictionary is a one-to-one mapping of category indices to complex values. Returns {@code null} for unknown
 * indices such as {@link CategoricalReader#MISSING_CATEGORY}. All known indices are mapped to distinct object values.
 * <p>
 * In case it has at most two non-null object values, a dictionary can be boolean, see {@link #isBoolean()}.
 *
 * @author Gisa Meier
 */
public class Dictionary<T> implements Iterable<Dictionary.Entry<T>> {

	/**
	 * An entry of a {@link Dictionary}, consisting of a category index and the associated object value. The entries
	 * are mutable and will be reused in iterators, therefore they should not be stored.
	 */
	public static final class Entry<T> {

		private int index;
		private T value;

		/**
		 * Returns the category index.
		 *
		 * @return the category index
		 */
		public int getIndex() {
			return index;
		}

		/**
		 * Returns the dictionary value at the category index.
		 *
		 * @return the object value
		 */
		public T getValue() {
			return value;
		}
	}


	private final List<T> dictionary;
	private final int unused;

	/**
	 * @param dictionary
	 * 		list with {@code null} at first place and no other entries {@code null}
	 */
	Dictionary(List<T> dictionary) {
		this.dictionary = dictionary;
		unused = 0;
	}

	/**
	 * @param dictionary
	 * 		list with {@code null} at first place and unused-many other entries but not the last
	 * @param unused
	 * 		the number of {@code null} entries in the list apart from the first entry
	 */
	Dictionary(List<T> dictionary, int unused) {
		this.dictionary = dictionary;
		this.unused = unused;
	}

	/**
	 * Returns the object value associated to the given category index. Returns {@code null} for {@link
	 * CategoricalReader#MISSING_CATEGORY} and for other indices that are not associated to an object.
	 *
	 * @param categoryIndex
	 * 		the index for which to retrieve the value from this dictionary
	 * @return the value associated to the category index
	 */
	public T get(int categoryIndex) {
		if (categoryIndex < 0 || categoryIndex >= dictionary.size()) {
			return null;
		}
		return dictionary.get(categoryIndex);
	}

	/**
	 * Creates a new map from object value to category index. {@code null} is always mapped to {@code 0} even if {@link
	 * #get(int)} returns {@code null} for several category indices.
	 *
	 * @return the inverse of the dictionary
	 */
	public Map<T, Integer> createInverse() {
		Map<T, Integer> inverseDictionary = new HashMap<>();
		for (int i = 1; i < dictionary.size(); i++) {
			inverseDictionary.put(dictionary.get(i), i);
		}
		inverseDictionary.put(null, 0);
		return inverseDictionary;
	}

	/**
	 * Iterator for the non-null object values and their category indices in this dictionary in order of the category
	 * indices.
	 *
	 * @return an iterator for the dictionary values which never returns {@code null}
	 */
	@Override
	public Iterator<Entry<T>> iterator() {
		Iterator<T> iterator = dictionary.iterator();
		iterator.next();
		return new Iterator<Entry<T>>() {

			private int index = 1;
			private Entry<T> entry = new Entry<>();

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public Entry<T> next() {
				do {
					entry.index = index++;
					entry.value = iterator.next();
				} while(entry.value == null);
				return entry;
			}
		};
	}

	/**
	 * The size of the map from category index to object value, i.e., the number of category indices inside the range
	 * {@code [0, maximalIndex()]} that are not mapped to {@code null}.
	 *
	 * @return the size of the dictionary
	 */
	public int size() {
		return dictionary.size() - unused - 1;
	}

	/**
	 * Returns the maximal category index that is associated to an object or {@code 0}. Note that not every category
	 * index below the maximal index must have an associated object value, there can be gaps.
	 *
	 * @return the maximal category index that can be used to call {@link #get(int)} without resulting in an exception
	 */
	public int maximalIndex() {
		return dictionary.size() - 1;
	}

	/**
	 * Whether this dictionary knows about positive and negative values. Iff this returns {@code false}, the methods
	 * {@link #hasPositive()}, {@link #hasNegative()}, {@link #getPositiveIndex()}, {@link #getNegativeIndex()} are not
	 * supported.
	 *
	 * @return whether this dictionary is boolean
	 */
	public boolean isBoolean() {
		return false;
	}

	/**
	 * Returns {@code true} if the boolean dictionary has a positive index.
	 *
	 * @return whether a positive index exists
	 * @throws UnsupportedOperationException
	 * 		if {@link #isBoolean()} returns {@code false}
	 */
	public boolean hasPositive() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns {@code true} if the boolean dictionary has a negative index.
	 *
	 * @return whether a negative index exists
	 * @throws UnsupportedOperationException
	 * 		if {@link #isBoolean()} returns {@code false}
	 */
	public boolean hasNegative() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns the positive index if this dictionary {@link #hasPositive()} or {@link BooleanDictionary#NO_ENTRY}.
	 *
	 * @return whether a positive index exists
	 * @throws UnsupportedOperationException
	 * 		if {@link #isBoolean()} returns {@code false}
	 */
	public int getPositiveIndex() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns the negative index if this dictionary {@link #hasNegative()} or {@link BooleanDictionary#NO_ENTRY}.
	 *
	 * @return whether a negative index exists
	 * @throws UnsupportedOperationException
	 * 		if {@link #isBoolean()} returns {@code false}
	 */
	public int getNegativeIndex() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Creates a new boolean dictionary with the given positive value.
	 *
	 * @param positiveValue
	 * 		the new positive value
	 * @return a boolean dictionary with the given positive index
	 */
	BooleanDictionary<T> toBoolean(T positiveValue) {
		int positiveIndex = BooleanDictionary.NO_ENTRY;
		if(positiveValue!=null) {
			for (int i = 0; i < dictionary.size(); i++) {
				if (positiveValue.equals(dictionary.get(i))){
					positiveIndex = i;
					break;
				}
			}
		}
		return new BooleanDictionary<>(dictionary, positiveIndex);
	}

	/**
	 * Returns the underlying list of object values as immutable list.
	 *
	 * @return the immutable list of object values
	 */
	List<T> getValueList() {
		return Collections.unmodifiableList(dictionary);
	}

	@Override
	public String toString() {
		return "Dictionary " +	dictionary;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Dictionary<?> that = (Dictionary<?>) o;
		return Objects.equals(dictionary, that.dictionary);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dictionary);
	}

}
