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

package com.rapidminer.belt.util;

import java.util.Objects;


/**
 * A column annotation in the form of a single {@code String}. There are no restrictions on the content of the
 * annotation other than being non-null.
 *
 * @author Michael Knopf
 */
public class ColumnAnnotation implements ColumnMetaData {

	private static final String TYPE_ID = "com.rapidminer.belt.meta.column.annotation";

	private static final int TO_STRING_CUTOFF = 50;

	private final String annotation;

	public ColumnAnnotation(String annotation) {
		this.annotation = Objects.requireNonNull(annotation, "Annotation must not be null");
	}

	/**
	 * Returns the column annotation.
	 *
	 * @return the annotation
	 */
	public String annotation() {
		return annotation;
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || getClass() != other.getClass()) {
			return false;
		}
		ColumnAnnotation that = (ColumnAnnotation) other;
		return Objects.equals(annotation, that.annotation);
	}

	@Override
	public int hashCode() {
		return 197 * annotation.hashCode();
	}

	@Override
	public String toString() {
		if (annotation.length() > TO_STRING_CUTOFF) {
			return annotation.substring(0, TO_STRING_CUTOFF - 3) + "...";
		} else {
			return annotation;
		}
	}

	@Override
	public Uniqueness uniqueness() {
		return Uniqueness.COLUMN;
	}

	@Override
	public String type() {
		return TYPE_ID;
	}

}
