/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
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

package com.rapidminer.belt.column.type;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;


/**
 * Immutable {@code Set<String>} that never contains {@code null} to use as a cell type. Some of the constructors
 * taking lambdas allow to construct the {@link StringSet} without first constructing a collection that is copied
 * again for immutability.
 *
 * @author Gisa Meier
 */
public final class StringSet implements Set<String>, Comparable<StringSet> {

	private final Set<String> set;
	
	/** hashCode for immutable set is stored to make {@link #compareTo(StringSet)} faster */
	private final int hashCode;

	/**
	 * Creates a new immutable string set by copying the given collection or, in case it is {@code null}, creating an
	 * empty set. {@code null} entries are removed.
	 *
	 * @param collection
	 * 		the collection to copy, can be {@code null}
	 */
	public StringSet(Collection<String> collection) {
		if (collection != null) {
			set = new LinkedHashSet<>();
			set.addAll(collection);
			set.remove(null);
		} else {
			set = Collections.emptySet();
		}
		hashCode = set.hashCode();
	}

	/**
	 * Creates a new string set by applying the preprocessor to every element of the collection. Preprocessed elements
	 * are only added if they are not {@code null}. If the collection is {@code null} an empty set is created. If the
	 * preprocessor is {@code null} no preprocessing takes place.
	 *
	 * @param collection
	 * 		the collection containing the elements to add after preprocessing
	 * @param preprocessor
	 * 		the function to apply to the element before adding them
	 */
	public StringSet(Collection<String> collection, UnaryOperator<String> preprocessor) {
		if (collection != null && preprocessor != null) {
			set = new LinkedHashSet<>();
			for (String entry : collection) {
				String apply = preprocessor.apply(entry);
				if (apply != null) {
					set.add(apply);
				}
			}
		} else if (collection != null) {
			set = new LinkedHashSet<>();
			set.addAll(collection);
			set.remove(null);
		} else {
			set = Collections.emptySet();
		}
		hashCode = set.hashCode();
	}

	/**
	 * Applies the supplier to the indices from {@code 0} (inclusive) to {@code limit} (exclusive) and adds the result
	 * to the string set if it is not {@code null}.
	 *
	 * @param supplier
	 * 		the function creating an element given an index
	 * @param limit
	 * 		the limit until which to apply the supplier (not inclusive)
	 */
	public StringSet(IntFunction<String> supplier, int limit) {
		set = new LinkedHashSet<>();
		if (supplier != null) {
			for (int i = 0; i < limit; i++) {
				String value = supplier.apply(i);
				if (value != null) {
					set.add(value);
				}
			}
		}
		hashCode = set.hashCode();
	}

	@Override
	public int size() {
		return set.size();
	}

	@Override
	public boolean isEmpty() {
		return set.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return set.contains(o);
	}

	@Override
	public Iterator<String> iterator() {
		return new Iterator<String>() {
			private final Iterator<String> i = set.iterator();

			@Override
			public boolean hasNext() {
				return i.hasNext();
			}

			@Override
			public String next() {
				return i.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void forEachRemaining(Consumer<? super String> action) {
				// Use backing collection version
				i.forEachRemaining(action);
			}
		};
	}

	@Override
	public Object[] toArray() {
		return set.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return set.toArray(a);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return set.containsAll(c);
	}

	@Override
	public Spliterator<String> spliterator() {
		return set.spliterator();
	}

	@Override
	public Stream<String> stream() {
		return set.stream();
	}

	@Override
	public Stream<String> parallelStream() {
		return set.parallelStream();
	}

	@Override
	public void forEach(Consumer<? super String> action) {
		set.forEach(action);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StringSet strings = (StringSet) o;
		return set.equals(strings.set);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public String toString() {
		return set.toString();
	}

	@Override
	public int compareTo(StringSet otherSet) {
		int len1 = set.size();
		int len2 = otherSet.size();
		if (len1 != len2) {
			return len1 - len2;
		}
		//for same length use the hashcode, equal sets have the same hashcode
		return this.hashCode - otherSet.hashCode;
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(String s) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeIf(Predicate<? super String> filter) {
		throw new UnsupportedOperationException();
	}

}
