/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2021 RapidMiner GmbH
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;


/**
 * Immutable {@code List<String>} that never contains {@code null} to use as a cell type. Some of the constructors
 * taking lambdas allow to construct the {@link StringList} without first constructing a collection that is copied again
 * for immutability.
 *
 * @author Gisa Meier
 */
public final class StringList implements List<String>, Comparable<StringList> {

	private final List<String> list;

	/**
	 * Creates a new immutable string list by copying the given collection or, in case it is {@code null}, creating an
	 * empty list. {@code null} entries are not added.
	 *
	 * @param collection
	 * 		the collection to copy, can be {@code null}
	 */
	public StringList(Collection<String> collection) {
		if (collection != null) {
			list = new ArrayList<>();
			for (String s : collection) {
				if (s != null) {
					list.add(s);
				}
			}
		} else {
			list = Collections.emptyList();
		}
	}

	/**
	 * Creates a new string list by applying the preprocessor to every element of the collection. Preprocessed elements
	 * are only added if they are not {@code null}. If the collection is {@code null} an empty list is created. If the
	 * preprocessor is {@code null} no preprocessing takes place.
	 *
	 * @param collection
	 * 		the collection containing the elements to add after preprocessing
	 * @param preprocessor
	 * 		the function to apply to the element before adding them
	 */
	public StringList(Collection<String> collection, UnaryOperator<String> preprocessor) {
		if (collection != null && preprocessor != null) {
			list = new ArrayList<>();
			for (String entry : collection) {
				String apply = preprocessor.apply(entry);
				if (apply != null) {
					list.add(apply);
				}
			}
		} else if (collection != null) {
			list = new ArrayList<>();
			for (String s : collection) {
				if (s != null) {
					list.add(s);
				}
			}
		} else {
			list = Collections.emptyList();
		}
	}

	/**
	 * Applies the supplier to the indices from {@code 0} (inclusive) to {@code limit} (exclusive) and adds the result
	 * to the string list if it is not {@code null}.
	 *
	 * @param supplier
	 * 		the function creating an element given an index
	 * @param limit
	 * 		the limit until which to apply the supplier (not inclusive)
	 */
	public StringList(IntFunction<String> supplier, int limit) {
		list = new ArrayList<>();
		if (supplier != null) {
			for (int i = 0; i < limit; i++) {
				String value = supplier.apply(i);
				if (value != null) {
					list.add(value);
				}
			}
		}
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return list.contains(o);
	}

	@Override
	public Iterator<String> iterator() {
		return new Iterator<String>() {
			private final Iterator<String> i = list.iterator();

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
		return list.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return list.toArray(a);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return list.containsAll(c);
	}

	@Override
	public Spliterator<String> spliterator() {
		return list.spliterator();
	}

	@Override
	public Stream<String> stream() {
		return list.stream();
	}

	@Override
	public Stream<String> parallelStream() {
		return list.parallelStream();
	}

	@Override
	public void forEach(Consumer<? super String> action) {
		list.forEach(action);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StringList strings = (StringList) o;
		return list.equals(strings.list);
	}

	@Override
	public int hashCode() {
		return list.hashCode();
	}

	@Override
	public String get(int index) {
		return list.get(index);
	}

	@Override
	public int indexOf(Object o) {
		return list.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return list.lastIndexOf(o);
	}

	@Override
	public ListIterator<String> listIterator() {
		return listIterator(0);
	}

	@Override
	public ListIterator<String> listIterator(int index) {
		return new ListIterator<String>() {
			private final ListIterator<String> it
					= list.listIterator(index);

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public String next() {
				return it.next();
			}

			@Override
			public int nextIndex() {
				return it.nextIndex();
			}

			@Override
			public boolean hasPrevious() {
				return it.hasPrevious();
			}

			@Override
			public String previous() {
				return it.previous();
			}

			@Override
			public int previousIndex() {
				return it.previousIndex();
			}

			@Override
			public void add(String e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void set(String e) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public void forEachRemaining(Consumer<? super String> action) {
				it.forEachRemaining(action);
			}
		};
	}

	@Override
	public List<String> subList(int fromIndex, int toIndex) {
		return Collections.unmodifiableList(list.subList(fromIndex, toIndex));
	}

	@Override
	public String toString() {
		return list.toString();
	}

	@Override
	public int compareTo(StringList otherList) {
		int len1 = list.size();
		int len2 = otherList.size();
		int min = Math.min(len1, len2);
		for (int i = 0; i < min; i++) {
			int compare = list.get(i).compareTo(otherList.get(i));
			if (compare != 0) {
				return compare;
			}
		}
		// same on min elements, shortest is smaller
		return len1 - len2;
	}

	@Override
	public String set(int index, String element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void add(int index, String element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String remove(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(int index, Collection<? extends String> c) {
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
