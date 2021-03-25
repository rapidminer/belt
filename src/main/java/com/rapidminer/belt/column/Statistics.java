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


import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.Workload;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.transform.Transformer;
import com.rapidminer.belt.util.Order;


/**
 * Utility methods for computing column statistics. See {@link Statistic} for supported statistics.
 *
 * @author Michael Knopf
 */
public class Statistics {

	private static final String MSG_NULL_COLUMN = "Column must not be null";

	/**
	 * Column statistics. Please note that depending on the column type, only a subset of statistics might be supported
	 * (see {@link #supported(Column, Statistic)}).
	 */
	public enum Statistic {
		/**
		 * The number of values (not counting missing values) in the column.
		 */
		COUNT,

		/**
		 * The minimum value in the column (if any).
		 */
		MIN,

		/**
		 * The maximum value in the column (if any).
		 */
		MAX,

		/**
		 * The arithmetic mean of the column values (if the count is non-zero).
		 */
		MEAN,

		/**
		 * The sample variance (unbiased variance) of column values (if the count is non-zero).
		 */
		VAR,

		/**
		 * The standard deviation of the column values (based on the sample variance).
		 */
		SD,

		/**
		 * The 25% percentile of the column values (if any) according to the linear interpolation method proposed by the
		 * National Institute for Standards and Technology (NIST).
		 */
		P25,

		/**
		 * The 50% percentile of the column values (if any) according to the linear interpolation method proposed by the
		 * National Institute for Standards and Technology (NIST).
		 */
		P50,

		/**
		 * The 75% percentile of the column values (if any) according to the linear interpolation method proposed by the
		 * National Institute for Standards and Technology (NIST).
		 */
		P75,

		/**
		 * The same as the 50% percentile ({@link #P50}).
		 */
		MEDIAN,

		/**
		 * (On of) the least frequent items in the column (if any).
		 */
		LEAST,

		/**
		 * (One of) the most frequent items in the column (if any).
		 */
		MODE,

		/**
		 * How often each category index appears in a categorical column.
		 */
		INDEX_COUNTS
	}


	/**
	 * Result container for a statistics computation. Not all values might be set depending on column type and
	 * statistic.
	 *
	 * <p>Statistics for columns of type {@link ColumnType#REAL} and {@link ColumnType#INTEGER_53_BIT} only make use of
	 * the numeric value (see {@link #getNumeric()}).
	 *
	 * <p>Statistics for columns of type {@link ColumnType#NOMINAL} make use of the categorical index and string value
	 * (see {@link #getCategorical()} and {@link #getObject()} respectively). An exception is the statistic
	 * {@link Statistic#COUNT} which makes use of the numeric value (see {@link #getNumeric()}) instead. The
	 * {@link Statistic#INDEX_COUNTS} is a special case as it returns a {@link CategoricalIndexCounts} accessible via
	 * {@link #getObject()}.
	 *
	 * <p>Statistics for columns of type {@link ColumnType#DATETIME} and {@link ColumnType#TIME} only make use of the
	 * {@link Instant} and {@link LocalTime} value respectively (see {@link #getObject()}). An exception is the
	 * statistic {@link Statistic#COUNT} which makes use of the numeric value (see {@link #getNumeric()}) instead.
	 *
	 * <p>Statistics for custom column types of category {@link Category#CATEGORICAL} and {@link Category#OBJECT} show
	 * the same behavior as nominal and date-time columns respectively.
	 */
	public static final class Result {

		private final double numericValue;
		private final int categoricalIndex;
		private final Object complexValue;

		private Result(double numericValue) {
			this.numericValue = numericValue;
			this.categoricalIndex = 0;
			this.complexValue = null;
		}

		private Result(double numericalValue, int categoricalIndex, Object complexValue) {
			this.numericValue = numericalValue;
			this.categoricalIndex = categoricalIndex;
			this.complexValue = complexValue;
		}

		/**
		 * Returns the numeric value of the result (if any). See the documentation of {@link Result} for details.
		 *
		 * @return the numeric value or {@link Double#NaN}
		 */
		public double getNumeric() {
			return numericValue;
		}

		/**
		 * Returns the categorical index of the result (if any). See the documentation of {@link Result} for details.
		 *
		 * @return the categorical index or {@link CategoricalReader#MISSING_CATEGORY}
		 */
		public int getCategorical() {
			return categoricalIndex;
		}

		/**
		 * Returns the complex value of the result (if any). See the documentation of {@link Result} for details.
		 *
		 * @return the complex value or {@code null}
		 */
		public Object getObject() {
			return complexValue;
		}

		/**
		 * Returns the complex value of the result (if any). See the documentation of {@link Result} for details.
		 *
		 * @param type
		 * 		the expected type
		 * @param <T>
		 * 		the expected type
		 * @return the complex value or {@code null}
		 * @throws ClassCastException
		 * 		if the complex value is not of the given type
		 */
		public <T> T getObject(Class<T> type) {
			return type.cast(complexValue);
		}

		@Override
		public boolean equals(Object other) {
			if (this == other) {
				return true;
			}
			if (other == null || getClass() != other.getClass()) {
				return false;
			}
			Result result = (Result) other;
			return Double.compare(result.numericValue, numericValue) == 0 &&
					categoricalIndex == result.categoricalIndex &&
					Objects.equals(complexValue, result.complexValue);
		}

		@Override
		public int hashCode() {
			return Objects.hash(numericValue, categoricalIndex, complexValue);
		}

	}

	/**
	 * Container for categorical index counts from categorical statistic calculations.
	 */
	public static final class CategoricalIndexCounts {
		private final int[] counts;

		private CategoricalIndexCounts(int[] counts) {
			this.counts = counts;
		}

		/**
		 * Returns how often a certain category index appears in the column this statistics was calculated for.
		 *
		 * @param categoryIndex
		 * 		the category index to check
		 * @return how often the category index appears
		 */
		public int countForIndex(int categoryIndex) {
			if (categoryIndex < 0 || categoryIndex >= counts.length) {
				return 0;
			}
			return counts[categoryIndex];
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			CategoricalIndexCounts that = (CategoricalIndexCounts) o;
			return Arrays.equals(counts, that.counts);
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(counts);
		}
	}

	/**
	 * Statistics supported for numeric columns.
	 */
	private static final Set<Statistic> NUMERIC = EnumSet.of(
			Statistic.COUNT,
			Statistic.MIN,
			Statistic.MAX,
			Statistic.MEAN,
			Statistic.VAR,
			Statistic.SD,
			Statistic.P25,
			Statistic.P50,
			Statistic.P75,
			Statistic.MEDIAN
	);

	/**
	 * Statistics supported for time column. The only difference to numeric columns is the lack of support of the
	 * variance. The reason for this is that the variance of the time of the day in nanoseconds might be outside the
	 * range of Java's {@link LocalTime}. The standard deviation however, will always be in range.
	 */
	private static final Set<Statistic> TIME = EnumSet.of(
			Statistic.COUNT,
			Statistic.MIN,
			Statistic.MAX,
			Statistic.MEAN,
			Statistic.SD,
			Statistic.P25,
			Statistic.P50,
			Statistic.P75,
			Statistic.MEDIAN
	);

	/**
	 * Statistics supported for datetime columns.
	 */
	private static final Set<Statistic> DATETIME = EnumSet.of(
			Statistic.COUNT,
			Statistic.MIN,
			Statistic.MAX
	);

	/**
	 * Statistics supported for generic categorical columns.
	 */
	private static final Set<Statistic> CATEGORICAL = EnumSet.of(
			Statistic.COUNT,
			Statistic.LEAST,
			Statistic.MODE,
			Statistic.INDEX_COUNTS
	);

	/**
	 * Statistics supported for generic object columns.
	 */
	private static final Set<Statistic> OBJECT = EnumSet.of(Statistic.COUNT);

	/**
	 * Container for the (intermediate) results of a reduction computing the count, min, max, and mean of a numerical
	 * column.
	 */
	private static final class NumericCounts {
		private int count = 0;
		private double min = Double.POSITIVE_INFINITY;
		private double max = Double.NEGATIVE_INFINITY;
		private double mean = Double.NaN;
		// Temporary fields that will need to be merged with the above values to obtain the final result
		private int tmpCount = 0;
		private double tmpSum = 0;
	}

	/**
	 * Container for the (intermediate) results of a reduction computing the count, variance, and standard deviation of
	 * a numerical column.
	 */
	private static final class NumericDeviation {
		private int count = 0;
		private double var = Double.NaN;
		private double sd = Double.NaN;
		// Temporary fields that will need to be merged with the above values to obtain the final result
		private int tmpCount = 0;
		private double tmpSumOfSquares = 0.0;
	}

	/**
	 * Container for 25%, 50% (median), and 75% percentiles of a numeric column.
	 */
	private static final class NumericPercentiles {
		private double p25 = Double.NaN;
		private double p50 = Double.NaN;
		private double p75 = Double.NaN;
	}

	/**
	 * Container for the (intermediate) results of a reduction computing the count, least used, and most used values
	 * of a categorical column.
	 */
	private static final class CategoricalCounts {
		private int count = 0;
		private int leastCount = Integer.MAX_VALUE;
		private int leastIndex = 0;
		private Object least = null;
		private int modeCount = Integer.MIN_VALUE;
		private int modeIndex = 0;
		private Object mode = null;
	}

	/**
	 * Container for the (intermediate) results of a reduction computing the count, min, and max value of a time column.
	 */
	private static final class InstantCounts {
		private int count;
		private Instant min = Instant.MAX;
		private Instant max = Instant.MIN;
	}

	private static final NumericDeviation DEFAULT_NUMERIC_DEVIATION = new NumericDeviation();
	private static final NumericPercentiles DEFAULT_NUMERIC_PERCENTILES = new NumericPercentiles();

	/**
	 * Checks whether the given statistic is supported for the given column.
	 *
	 * @param column
	 * 		the column
	 * @param statistic
	 * 		the statistic
	 * @return {@code true} iff the given statistic is supported
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public static boolean supported(Column column, Statistic statistic) {
		Objects.requireNonNull(column, MSG_NULL_COLUMN);
		Objects.requireNonNull(statistic, "Statistic must not be null");
		ColumnType<?> type = column.type();
		switch (type.id()) {
			case INTEGER_53_BIT:
			case REAL:
				return NUMERIC.contains(statistic);
			case TIME:
				return TIME.contains(statistic);
			case NOMINAL:
				return CATEGORICAL.contains(statistic);
			case DATE_TIME:
				return DATETIME.contains(statistic);
			default:
				// Decide based on category
				return supported(type.category(), statistic);
		}
	}

	private static boolean supported(Category category, Statistic statistic) {
		switch (category) {
			case CATEGORICAL:
				return CATEGORICAL.contains(statistic);
			case OBJECT:
				return OBJECT.contains(statistic);
			default:
				return false;
		}
	}

	/**
	 * Computes a single statistic for the given column. Please note that is strongly recommended to use
	 * {@link #compute(Column, Set, Context)} instead of this method whenever multiple statistics are to be computed.
	 *
	 * @param column
	 * 		the column
	 * @param statistic
	 * 		the statistic
	 * @param ctx
	 * 		the context
	 * @return the result
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the statistic is not supported for the given column (see {@link #supported(Column, Statistic)})
	 */
	public static Result compute(Column column, Statistic statistic, Context ctx) {
		Objects.requireNonNull(ctx, "Context must not be null");
		if (!supported(column, statistic)) {
			throw new UnsupportedOperationException("Unsupported statistics: " + statistic);
		}
		switch (column.type().id()) {
			case INTEGER_53_BIT:
			case REAL:
				return computeNumeric(column, statistic, ctx);
			case NOMINAL:
				return computeCategorical(column, statistic, ctx);
			case TIME:
				return computeTime(column, statistic, ctx);
			case DATE_TIME:
				return computeDateTime(column, statistic, ctx);
			default:
				return computeOther(column, statistic, ctx);
		}
	}

	private static Result computeOther(Column column, Statistic statistic, Context context) {
		switch (column.type().category()) {
			case CATEGORICAL:
				return computeCategorical(column, statistic, context);
			case OBJECT:
				return computeObject(column, statistic, context);
			default:
				throw new AssertionError();
		}
	}

	/**
	 * Computes one or more statistics for the given column.
	 *
	 * @param column
	 * 		the column
	 * @param statistics
	 * 		the statistics to compute
	 * @param ctx
	 * 		the context
	 * @return the result set
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the statistics is not supported for the given column (see {@link #supported(Column, Statistic)})
	 */
	public static Map<Statistic, Result> compute(Column column, Set<Statistic> statistics, Context ctx) {
		Objects.requireNonNull(column, MSG_NULL_COLUMN);
		Objects.requireNonNull(statistics, "Statistics must not be null");
		if (statistics.isEmpty()) {
			return Collections.emptyMap();
		}
		for (Statistic statistic : statistics) {
			if (!supported(column, statistic)) {
				throw new UnsupportedOperationException("Unsupported statistic: " + statistic);
			}
		}
		switch (column.type().id()) {
			case INTEGER_53_BIT:
			case REAL:
				return computeNumeric(column, statistics, ctx);
			case NOMINAL:
				return computeCategorical(column, statistics, ctx);
			case TIME:
				return computeTime(column, statistics, ctx);
			case DATE_TIME:
				return computeDateTime(column, statistics, ctx);
			default:
				return computeOther(column, statistics, ctx);
		}
	}

	private static Map<Statistic, Result> computeOther(Column column, Set<Statistic> statistics, Context ctx) {
		switch (column.type().category()) {
			case CATEGORICAL:
				return computeCategorical(column, statistics, ctx);
			case OBJECT:
				return computeObject(column, statistics, ctx);
			default:
				throw new AssertionError();
		}
	}

	private static Result computeNumeric(Column column, Statistic statistic, Context ctx) {
		// All statistics rely on a first pass computing simple counts.
		NumericCounts counts = computeNumericCounts(column, ctx);
		NumericDeviation deviation = DEFAULT_NUMERIC_DEVIATION;
		NumericPercentiles percentiles = DEFAULT_NUMERIC_PERCENTILES;
		switch (statistic) {
			case VAR:
			case SD:
				deviation = computeNumericDeviation(column, counts, ctx);
				break;
			case P25:
			case P50:
			case P75:
			case MEDIAN:
				percentiles = computeNumericPercentiles(column, counts.count);
				break;
			default:
				break;
		}
		return extractNumericStatistic(statistic, counts, deviation, percentiles);
	}

	private static Map<Statistic, Result> computeNumeric(Column column, Set<Statistic> statistics, Context ctx) {
		Map<Statistic, Result> resultMap = new EnumMap<>(Statistic.class);
		NumericCounts counts = computeNumericCounts(column, ctx);
		NumericDeviation deviation = DEFAULT_NUMERIC_DEVIATION;
		NumericPercentiles percentiles = DEFAULT_NUMERIC_PERCENTILES;
		for (Statistic stat : statistics) {
			Result cachedResult = column.getStat(stat);
			if (cachedResult != null) {
				resultMap.put(stat, cachedResult);
			} else {
				// not cached yet, need to compute it
				switch (stat) {
					case VAR:
					case SD:
						if (deviation == DEFAULT_NUMERIC_DEVIATION) {
							deviation = computeNumericDeviation(column, counts, ctx);
						}
						break;
					case P25:
					case P50:
					case P75:
					case MEDIAN:
						if (percentiles == DEFAULT_NUMERIC_PERCENTILES) {
							percentiles = computeNumericPercentiles(column, counts.count);
						}
						break;
					default:
						break;
				}
				resultMap.put(stat, extractNumericStatistic(stat, counts, deviation, percentiles));
			}
		}
		return resultMap;
	}

	private static Result extractNumericStatistic(Statistic statistic, NumericCounts countMinMax,
												  NumericDeviation varStd, NumericPercentiles percentiles) {
		double numericValue;
		switch (statistic) {
			case COUNT:
				numericValue = countMinMax.count;
				break;
			case MIN:
				numericValue = countMinMax.min;
				break;
			case MAX:
				numericValue = countMinMax.max;
				break;
			case MEAN:
				numericValue = countMinMax.mean;
				break;
			case VAR:
				numericValue = varStd.var;
				break;
			case SD:
				numericValue = varStd.sd;
				break;
			case P25:
				numericValue = percentiles.p25;
				break;
			case P50:
			case MEDIAN:
				numericValue = percentiles.p50;
				break;
			case P75:
				numericValue = percentiles.p75;
				break;
			default:
				throw new AssertionError();
		}
		return new Result(numericValue);
	}

	private static Result computeTime(Column column, Statistic statistic, Context ctx) {
		// Time columns are de-facto integer (nanoseconds of the day).
		Result result = computeNumeric(column, statistic, ctx);
		return convertNumericToLocalTime(statistic, result);
	}

	private static Map<Statistic, Result> computeTime(Column column, Set<Statistic> statistics, Context ctx) {
		// Time columns are de-facto integer (nanoseconds of the day).
		Map<Statistic, Result> numericResults = computeNumeric(column, statistics, ctx);
		Map<Statistic, Result> timeResults = new EnumMap<>(Statistic.class);
		for (Map.Entry<Statistic, Result> entry : numericResults.entrySet()) {
			Statistic statistic = entry.getKey();
			Result result = entry.getValue();
			timeResults.put(statistic, convertNumericToLocalTime(statistic, result));
		}
		return timeResults;
	}

	private static Result convertNumericToLocalTime(Statistic statistic, Result result) {
		if (statistic == Statistic.COUNT) {
			return result;
		} else {
			long nanos = (long) result.numericValue;
			LocalTime time = Double.isNaN(result.getNumeric()) ? null : LocalTime.ofNanoOfDay(nanos);
			return new Result(Double.NaN, -1, time);
		}
	}

	private static NumericCounts computeNumericCounts(Column column, Context ctx) {

		// use cached statistics if possible
		NumericCounts cachedCounts = getCachedNumericCounts(column);
		if (cachedCounts != null) {
			return cachedCounts;
		}
		synchronized (column) {
			// check cached statistics again
			cachedCounts = getCachedNumericCounts(column);
			if (cachedCounts != null) {
				return cachedCounts;
			}

			// Compute the means per batch and merge results in the combiner to increase the numeric stability.
			Transformer transformer = new Transformer(column).workload(Workload.MEDIUM);

			NumericCounts result = transformer.reduceNumeric(
					NumericCounts::new,
					(stats, value) -> {
						if (!Double.isNaN(value)) {
							stats.min = Double.min(stats.min, value);
							stats.max = Double.max(stats.max, value);
							stats.tmpCount++;
							stats.tmpSum += value;
						}
					},
					(statsA, statsB) -> {
						// Compute count and mean from temporary fields.
						computeCountsFromTmpFields(statsA);
						computeCountsFromTmpFields(statsB);
						// Combine the two mean values.
						if (statsB.count > 0) {
							if (statsA.count > 0) {
								double weight = (double) statsA.count / (statsA.count + statsB.count);
								statsA.mean = weight * statsA.mean + (1.0 - weight) * statsB.mean;
							} else {
								statsA.mean = statsB.mean;
							}
						}
						// Update counts etc.
						statsA.count += statsB.count;
						statsA.min = Double.min(statsA.min, statsB.min);
						statsA.max = Double.max(statsA.max, statsB.max);
					},
					ctx
			);

			// In case of a sequential execution, the combiner might not be invoked at all.
			computeCountsFromTmpFields(result);

			if (result.count == 0) {
				// Min and max are still be set to POSITIVE_INFINITY and NEGATIVE_INFINITY respectively.
				result.min = Double.NaN;
				result.max = Double.NaN;
			} else if (result.mean < result.min) {
				// This can happen if a temporary sum (see reduction) becomes so small that we lose precision or end
				// up with
				// an infinite value. If the mean is still finite, it must be close to the minimum, otherwise it
				// could be
				// everywhere in between the minimum and maximum value.
				result.mean = Double.isFinite(result.mean) ? result.min : Double.NaN;
			} else if (result.mean > result.max) {
				// See explanation for the case above.
				result.mean = Double.isFinite(result.mean) ? result.max : Double.NaN;
			}

			cacheNumericCounts(column, result);
			return result;
		}
	}

	private static void computeCountsFromTmpFields(NumericCounts stats) {
		// Merge any temporary values (e.g., aggregated during the reduction of a single batch) with the actual counts.
		if (stats.tmpCount > 0) {
			double tmpMean = stats.tmpSum / stats.tmpCount;
			if (stats.count == 0) {
				stats.mean = tmpMean;
				stats.count = stats.tmpCount;
			} else {
				double weight = (double) stats.count / (stats.count + stats.tmpCount);
				stats.mean = weight * stats.mean + (1.0 - weight) * tmpMean;
				stats.count += stats.tmpCount;
			}
			// Reset temporary fields (clear temporary results).
			stats.tmpCount = 0;
			stats.tmpSum = 0.0;
		}
	}

	private static NumericDeviation computeNumericDeviation(Column column, NumericCounts counts, Context ctx) {
		NumericDeviation cachedDeviation = getCachedNumericDeviation(column);
		if (cachedDeviation != null) {
			return cachedDeviation;
		}

		synchronized (column) {
			cachedDeviation = getCachedNumericDeviation(column);
			if (cachedDeviation != null) {
				return cachedDeviation;
			}

			// We want to compute the sample variance which is only defined for two or more samples (division by n-1).
			// Furthermore, an infinite mean does not allow for a meaningful computation of the variance. To begin with,
			// it might be both an actual infinite value or a finite one outside of double range.
			if (counts.count < 2 || Double.isNaN(counts.mean) || Double.isInfinite(counts.mean)) {
				NumericDeviation result = new NumericDeviation();
				result.count = counts.count;
				return result;
			}

			// Compute the population (not sample) variance per batch and merge results in the combiner to increase the
			// numeric stability.
			double mean = counts.mean;
			Transformer transformer = new Transformer(column).workload(Workload.MEDIUM);
			NumericDeviation result = transformer.reduceNumeric(
					NumericDeviation::new,
					(stats, value) -> {
						if (!Double.isNaN(value)) {
							double diff = value - mean;
							stats.tmpSumOfSquares += diff * diff;
							stats.tmpCount++;
						}
					},
					(statsA, statsB) -> {
						// Compute variance from temporary fields.
						computeVarianceFromTmpFields(statsA);
						computeVarianceFromTmpFields(statsB);
						// Combine the two variance values.
						if (statsB.count > 0) {
							if (statsA.count > 0) {
								double weight = (double) statsA.count / (statsA.count + statsB.count);
								statsA.var = weight * statsA.var + (1.0 - weight) * statsB.var;
							} else {
								statsA.var = statsB.var;
							}
						}
						// Update count.
						statsA.count += statsB.count;
					},
					ctx
			);

			// In case of a sequential execution, the combiner might not be invoked at all.
			computeVarianceFromTmpFields(result);

			// The population variance is defined as 1/n * sum(...), the sample variance as 1/(n-1) * sum(...). Thus, we
			// need to scale the variance by n/(n-1).
			double correction = ((double) result.count) / (result.count - 1);
			result.var *= correction;
			result.sd = Math.sqrt(result.var);

			cacheNumericDeviation(column, result);

			return result;
		}
	}

	private static void computeVarianceFromTmpFields(NumericDeviation stats) {
		// Merge any temporary values (e.g., aggregated during the reduction of a single batch) with the actual values.
		if (stats.tmpCount > 0) {
			double tmpVariance = stats.tmpSumOfSquares / stats.tmpCount;
			if (stats.count == 0) {
				stats.var = tmpVariance;
				stats.count = stats.tmpCount;
			} else {
				double weight = (double) stats.count / (stats.count + stats.tmpCount);
				stats.var = weight * stats.var + (1.0 - weight) * tmpVariance;
				stats.count += stats.tmpCount;
			}
			stats.tmpCount = 0;
			stats.tmpSumOfSquares = 0;
		}
	}

	private static NumericPercentiles computeNumericPercentiles(Column column, int count) {
		NumericPercentiles cachedPercentiles = getCachedNumericPercentiles(column);
		if (cachedPercentiles != null) {
			return cachedPercentiles;
		}
		synchronized (column) {
			cachedPercentiles = getCachedNumericPercentiles(column);
			if (cachedPercentiles != null) {
				return cachedPercentiles;
			}

			NumericPercentiles percentiles = new NumericPercentiles();
			if (count == 0) {
				return percentiles;
			}
			// A count of one only tells us that there is a single non-missing value. It might still be part of a larger
			// column (of otherwise missing values).
			Column sorted = column.map(column.sort(Order.ASCENDING), true);
			NumericReader reader = Readers.numericReader(sorted);
			percentiles.p25 = computePercentile(reader, count, 0.25);
			percentiles.p50 = computePercentile(reader, count, 0.5);
			percentiles.p75 = computePercentile(reader, count, 0.75);

			cacheNumericPercentiles(column, percentiles);

			return percentiles;
		}
	}

	/**
	 * Computes the (interpolated) pth percentile of the given n (sorted) values. Please note that there is not a
	 * standard universally accepted way to interpolate percentiles. This implementation uses the method proposed by
	 * the National Institute of Standards and Technology (NIST) as described in its Engineering Statistics Handbook.
	 *
	 * @param reader
	 * 		the reader for the sorted values
	 * @param n
	 * 		the number of non-missing values
	 * @param p
	 * 		the percentile to compute
	 * @return the pth percentile
	 */
	private static double computePercentile(NumericReader reader, int n, double p) {
		double rank = p * (n + 1);
		int index = (int) rank;
		if (index < 1) {
			reader.setPosition(Readers.BEFORE_FIRST_ROW);
			return reader.read();
		} else if (index < n) {
			reader.setPosition(index - 2);
			double weight = rank - index;
			double value = reader.read();
			value += weight * (reader.read() - value);
			return value;
		} else {
			reader.setPosition(n - 2);
			return reader.read();
		}
	}

	private static Result computeCategorical(Column column, Statistic statistic, Context ctx) {
		CategoricalCounts counts;
		switch (statistic) {
			case COUNT:
				return new Result(computeCategoricalElementCount(column, ctx));
			case LEAST:
				counts = computeCategoricalCounts(column, ctx);
				return new Result(counts.leastCount, counts.leastIndex, counts.least);
			case MODE:
				counts = computeCategoricalCounts(column, ctx);
				return new Result(counts.modeCount, counts.modeIndex, counts.mode);
			case INDEX_COUNTS:
				Dictionary dictionary = column.getDictionary();
				int nValues = dictionary.maximalIndex() + 1;
				int[] indexCounts = calculateIndexCounts(column, ctx, nValues);
				CategoricalIndexCounts valueCount = new CategoricalIndexCounts(indexCounts);
				return new Result(Double.NaN, 0, valueCount);
			default:
				throw new AssertionError();
		}
	}

	private static Map<Statistic, Result> computeCategorical(Column column, Set<Statistic> statistics, Context ctx) {
		Map<Statistic, Result> resultMap = new EnumMap<>(Statistic.class);
		if (statistics.size() == 1 && statistics.contains(Statistic.COUNT)) {
			resultMap.put(Statistic.COUNT, new Result(computeCategoricalElementCount(column, ctx)));
		} else if (statistics.contains(Statistic.INDEX_COUNTS)) {
			Dictionary dictionary = column.getDictionary();
			int nValues = dictionary.maximalIndex() + 1;
			int[] indexCounts = calculateIndexCounts(column, ctx, nValues);
			CategoricalIndexCounts valueCount = new CategoricalIndexCounts(indexCounts);
			resultMap.put(Statistic.INDEX_COUNTS, new Result(Double.NaN, 0, valueCount));
			if (statistics.size() > 1) {
				CategoricalCounts cachedCategoricalCounts = getCachedCategoricalCounts(column);
				CategoricalCounts counts = cachedCategoricalCounts != null ? cachedCategoricalCounts :
						extractCategoricalCounts(dictionary, nValues, indexCounts);
				for (Statistic statistic : statistics) {
					if (statistic != Statistic.INDEX_COUNTS) {
						resultMap.put(statistic, extractCategoricalStatistic(statistic, counts));
					}
				}
			}
		} else {
			CategoricalCounts counts = computeCategoricalCounts(column, ctx);
			for (Statistic statistic : statistics) {
				resultMap.put(statistic, extractCategoricalStatistic(statistic, counts));
			}
		}
		return resultMap;
	}

	private static Result extractCategoricalStatistic(Statistic statistic, CategoricalCounts counts) {
		switch (statistic) {
			case COUNT:
				return new Result(counts.count);
			case LEAST:
				return new Result(counts.leastCount, counts.leastIndex, counts.least);
			case MODE:
				return new Result(counts.modeCount, counts.modeIndex, counts.mode);
			default:
				throw new AssertionError();
		}
	}

	private static int computeCategoricalElementCount(Column column, Context ctx) {
		if (column.getStat(Statistic.COUNT) != null) {
			return (int) column.getStat(Statistic.COUNT).numericValue;
		}
		synchronized (column) {
			if (column.getStat(Statistic.COUNT) != null) {
				return (int) column.getStat(Statistic.COUNT).numericValue;
			}
			// In case we are only interested in the element count, use a primitive (light weight) reducer
			int result = new Transformer(column).reduceCategorical(
					CategoricalReader.MISSING_CATEGORY,
					(count, index) -> index == CategoricalReader.MISSING_CATEGORY ? count : count + 1,
					(countA, countB) -> countA + countB,
					ctx);
			column.cacheStat(Statistic.COUNT, new Result(result));
			return result;
		}
	}

	private static CategoricalCounts computeCategoricalCounts(Column column, Context ctx) {
		CategoricalCounts cachedCounts = getCachedCategoricalCounts(column);
		if (cachedCounts != null) {
			return cachedCounts;
		}
		synchronized (column) {
			cachedCounts = getCachedCategoricalCounts(column);
			if (cachedCounts != null) {
				return cachedCounts;
			}
			// Count occurrences of each dictionary index.
			Dictionary dictionary = column.getDictionary();
			int nValues = dictionary.maximalIndex() + 1;
			int[] indexCounts = calculateIndexCounts(column, ctx, nValues);
			CategoricalCounts result = extractCategoricalCounts(dictionary, nValues, indexCounts);
			cacheCategoricalCounts(column, result);
			return result;
		}
	}

	private static CategoricalCounts extractCategoricalCounts(Dictionary dictionary, int nValues,
															  int[] indexCounts) {
		// Lookup most/least frequent values (if any).
		CategoricalCounts counts = new CategoricalCounts();
		for (int index = 1; index < nValues; index++) {
			int indexCount = indexCounts[index];
			if (indexCount == 0) {
				// Dictionary index not used in column.
				continue;
			}
			counts.count += indexCount;
			if (indexCount < counts.leastCount) {
				counts.leastIndex = index;
				counts.leastCount = indexCount;
			}
			if (indexCount > counts.modeCount) {
				counts.modeIndex = index;
				counts.modeCount = indexCount;
			}
		}

		if (counts.leastIndex == 0) {
			counts.leastCount = 0;
		} else {
			counts.least = dictionary.get(counts.leastIndex);
		}

		if (counts.modeIndex == 0) {
			counts.modeCount = 0;
		} else {
			counts.mode = dictionary.get(counts.modeIndex);
		}

		return counts;
	}

	private static int[] calculateIndexCounts(Column column, Context ctx, int nValues) {
		Transformer transformer = new Transformer(column).workload(Workload.MEDIUM);
		return transformer.reduceCategorical(
				() -> new int[nValues],
				(counts, index) -> counts[index]++,
				(countsA, countsB) -> {
					for (int index = 0; index < nValues; index++) {
						countsA[index] += countsB[index];
					}
				},
				ctx);
	}

	private static Result computeDateTime(Column column, Statistic statistic, Context ctx) {
		switch (statistic) {
			case COUNT:
				return new Result(computeObjectCount(column, ctx), 0, null);
			case MIN:
				return new Result(Double.NaN, 0, computeInstantCounts(column, ctx).min);
			case MAX:
				return new Result(Double.NaN, 0, computeInstantCounts(column, ctx).max);
			default:
				throw new AssertionError();
		}
	}

	private static Map<Statistic, Result> computeDateTime(Column column, Set<Statistic> statistics, Context ctx) {
		Map<Statistic, Result> resultMap = new EnumMap<>(Statistic.class);
		if (statistics.size() == 1 && statistics.contains(Statistic.COUNT)) {
			resultMap.put(Statistic.COUNT, new Result(computeObjectCount(column, ctx), 0, null));
		} else {
			InstantCounts counts = computeInstantCounts(column, ctx);
			for (Statistic statistic : statistics) {
				Result result;
				switch (statistic) {
					case COUNT:
						result = new Result(counts.count, 0, null);
						break;
					case MIN:
						result = new Result(Double.NaN, 0, counts.min);
						break;
					case MAX:
						result = new Result(Double.NaN, 0, counts.max);
						break;
					default:
						throw new AssertionError();
				}
				resultMap.put(statistic, result);
			}
		}
		return resultMap;
	}

	private static Result computeObject(Column column, Statistic statistic, Context ctx) {
		if (statistic != Statistic.COUNT) {
			throw new AssertionError();
		}
		return new Result(computeObjectCount(column, ctx), -1, null);
	}

	private static Map<Statistic, Result> computeObject(Column column, Set<Statistic> statistics, Context ctx) {
		// Reuse logic for single statistic, since we only support one.
		Map<Statistic, Result> resultMap = new EnumMap<>(Statistic.class);
		for (Statistic statistic : statistics) {
			resultMap.put(statistic, computeObject(column, statistic, ctx));
		}
		return resultMap;
	}

	private static int computeObjectCount(Column column, Context ctx) {
		Result cachedCount = column.getStat(Statistic.COUNT);
		if (cachedCount != null) {
			return (int) cachedCount.numericValue;
		}
		synchronized (column) {
			cachedCount = column.getStat(Statistic.COUNT);
			if (cachedCount != null) {
				return (int) cachedCount.numericValue;
			}

			Transformer transformer = new Transformer(column).workload(Workload.MEDIUM);
			int[] objectCount = transformer.reduceObjects(
					Object.class,
					() -> new int[1],
					(count, value) -> {
						if (value != null) {
							count[0]++;
						}
					},
					(countA, countB) -> countA[0] += countB[0],
					ctx
			);

			column.cacheStat(Statistic.COUNT, new Result(objectCount[0]));

			return objectCount[0];
		}
	}

	private static InstantCounts computeInstantCounts(Column column, Context ctx) {
		InstantCounts cachedCounts = getCachedInstantCounts(column);
		if (cachedCounts != null) {
			return cachedCounts;
		}

		synchronized (column) {
			cachedCounts = getCachedInstantCounts(column);
			if (cachedCounts != null) {
				return cachedCounts;
			}

			Transformer transformer = new Transformer(column).workload(Workload.MEDIUM);
			InstantCounts instantCounts = transformer.reduceObjects(
					Instant.class,
					InstantCounts::new,
					(counts, instant) -> {
						if (instant != null) {
							counts.count++;
							if (instant.compareTo(counts.min) < 0) {
								counts.min = instant;
							}
							if (instant.compareTo(counts.max) > 0) {
								counts.max = instant;
							}
						}
					},
					(countsA, countsB) -> {
						countsA.count += countsB.count;
						if (countsB.min.compareTo(countsA.min) < 0) {
							countsA.min = countsB.min;
						}
						if (countsB.max.compareTo(countsA.max) > 0) {
							countsA.max = countsB.max;
						}
					},
					ctx
			);
			if (instantCounts.count == 0) {
				instantCounts.min = null;
				instantCounts.max = null;
			}

			cacheInstantCounts(column, instantCounts);

			return instantCounts;
		}
	}

	/**
	 * Caches the given statistics in the given column.
	 *
	 * @param column
	 * 		the column used to cache the statistics
	 * @param instantCounts
	 * 		the statistics to cache
	 */
	private static void cacheInstantCounts(Column column, InstantCounts instantCounts) {
		column.cacheStat(Statistic.COUNT, new Result(instantCounts.count));
		column.cacheStat(Statistic.MIN, new Result(Double.NaN, 0, instantCounts.min));
		column.cacheStat(Statistic.MAX, new Result(Double.NaN, 0, instantCounts.max));
	}

	/**
	 * Returns the cached instant counts if they exist and {@code null} otherwise.
	 *
	 * @param column
	 * 		the column holding the cached instant counts
	 * @return the instant counts
	 */
	private static InstantCounts getCachedInstantCounts(Column column) {
		Result cachedCount = column.getStat(Statistic.COUNT);
		Result cachedMin = column.getStat(Statistic.MIN);
		Result cachedMax = column.getStat(Statistic.MAX);
		if (cachedCount != null && cachedMin != null && cachedMax != null) {
			InstantCounts result = new InstantCounts();
			result.count = (int) cachedCount.numericValue;
			result.max = (Instant) cachedMax.complexValue;
			result.min = (Instant) cachedMin.complexValue;
			return result;
		}
		return null;
	}

	/**
	 * Caches the given statistics in the given column.
	 *
	 * @param column
	 * 		the column used to cache the statistics
	 * @param numericCounts
	 * 		the statistics to cache
	 */
	private static void cacheNumericCounts(Column column, NumericCounts numericCounts) {
		column.cacheStat(Statistic.COUNT, new Result(numericCounts.count));
		column.cacheStat(Statistic.MIN, new Result(numericCounts.min));
		column.cacheStat(Statistic.MAX, new Result(numericCounts.max));
		column.cacheStat(Statistic.MEAN, new Result(numericCounts.mean));
	}

	/**
	 * Returns the cached numeric counts if they exist and {@code null} otherwise.
	 *
	 * @param column
	 * 		the column holding the cached numeric counts
	 * @return the numeric counts
	 */
	private static NumericCounts getCachedNumericCounts(Column column) {
		Result cachedCount = column.getStat(Statistic.COUNT);
		Result cachedMin = column.getStat(Statistic.MIN);
		Result cachedMax = column.getStat(Statistic.MAX);
		Result cachedMean = column.getStat(Statistic.MEAN);

		if (cachedCount != null && cachedMin != null && cachedMax != null && cachedMean != null) {
			NumericCounts cachedCounts = new NumericCounts();
			cachedCounts.count = (int) cachedCount.numericValue;
			cachedCounts.max = cachedMax.numericValue;
			cachedCounts.min = cachedMin.numericValue;
			cachedCounts.mean = cachedMean.numericValue;
			return cachedCounts;
		}
		return null;
	}

	/**
	 * Caches the given statistics in the given column.
	 *
	 * @param column
	 * 		the column used to cache the statistics
	 * @param deviation
	 * 		the statistics to cache
	 */
	private static void cacheNumericDeviation(Column column, NumericDeviation deviation) {
		column.cacheStat(Statistic.COUNT, new Result(deviation.count));
		column.cacheStat(Statistic.SD, new Result(deviation.sd));
		column.cacheStat(Statistic.VAR, new Result(deviation.var));
	}

	/**
	 * Returns the cached numeric deviation if they exist and {@code null} otherwise.
	 *
	 * @param column
	 * 		the column holding the cached numeric deviation
	 * @return the numeric deviation
	 */
	private static NumericDeviation getCachedNumericDeviation(Column column) {
		Result cachedCount = column.getStat(Statistic.COUNT);
		Result cachedSD = column.getStat(Statistic.SD);
		Result cachedVar = column.getStat(Statistic.VAR);
		if (cachedCount != null && cachedSD != null && cachedVar != null) {
			NumericDeviation cachedDeviation = new NumericDeviation();
			cachedDeviation.count = (int) cachedCount.numericValue;
			cachedDeviation.sd = cachedSD.numericValue;
			cachedDeviation.var = cachedVar.numericValue;
			return cachedDeviation;
		}
		return null;
	}

	/**
	 * Caches the given statistics in the given column.
	 *
	 * @param column
	 * 		the column used to cache the statistics
	 * @param percentiles
	 * 		the statistics to cache
	 */
	private static void cacheNumericPercentiles(Column column, NumericPercentiles percentiles) {
		column.cacheStat(Statistic.P25, new Result(percentiles.p25));
		column.cacheStat(Statistic.P50, new Result(percentiles.p50));
		column.cacheStat(Statistic.P75, new Result(percentiles.p75));
		column.cacheStat(Statistic.MEDIAN, new Result(percentiles.p50));
	}

	/**
	 * Returns the cached numeric percentiles if they exist and {@code null} otherwise.
	 *
	 * @param column
	 * 		the column holding the cached numeric percentiles
	 * @return the numeric percentiles
	 */
	private static NumericPercentiles getCachedNumericPercentiles(Column column) {
		Result cachedP25 = column.getStat(Statistic.P25);
		Result cachedP50 = column.getStat(Statistic.P50);
		Result cachedP75 = column.getStat(Statistic.P75);
		if (cachedP25 != null && cachedP50 != null && cachedP75 != null) {
			NumericPercentiles cachedPercentiles = new NumericPercentiles();
			cachedPercentiles.p25 = cachedP25.numericValue;
			cachedPercentiles.p50 = cachedP50.numericValue;
			cachedPercentiles.p75 = cachedP75.numericValue;
			return cachedPercentiles;
		}
		return null;
	}

	/**
	 * Caches the given statistics in the given column.
	 *
	 * @param column
	 * 		the column used to cache the statistics
	 * @param counts
	 * 		the statistics to cache
	 */
	private static void cacheCategoricalCounts(Column column, CategoricalCounts counts) {
		column.cacheStat(Statistic.COUNT, new Result(counts.count));
		column.cacheStat(Statistic.LEAST, new Result(counts.leastCount, counts.leastIndex, counts.least));
		column.cacheStat(Statistic.MODE, new Result(counts.modeCount, counts.modeIndex, counts.mode));
	}

	/**
	 * Returns the cached categorical counts if they exist and {@code null} otherwise.
	 *
	 * @param column
	 * 		the column holding the cached categorical counts
	 * @return the categorical counts
	 */
	private static CategoricalCounts getCachedCategoricalCounts(Column column) {
		Result cachedCount = column.getStat(Statistic.COUNT);
		Result cachedMode = column.getStat(Statistic.MODE);
		Result cachedLeast = column.getStat(Statistic.LEAST);
		if (cachedCount != null && cachedMode != null && cachedLeast != null) {
			CategoricalCounts result = new CategoricalCounts();
			result.count = (int) cachedCount.numericValue;

			result.mode = cachedMode.complexValue;
			result.modeCount = (int) cachedMode.numericValue;
			result.modeIndex = cachedMode.categoricalIndex;

			result.least = cachedLeast.complexValue;
			result.leastCount = (int) cachedLeast.numericValue;
			result.leastIndex = cachedLeast.categoricalIndex;

			return result;
		}
		return null;
	}

}
