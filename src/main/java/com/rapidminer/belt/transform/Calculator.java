package com.rapidminer.belt.transform;

/**
 * A transform that is called by a {@link ParallelExecutor} in order to do a calculation in parallel.
 *
 * @param <T>
 * 		the type of the result
 * @author Gisa Meier
 */
interface Calculator<T> {

	/**
	 * Initializes the transform. This is called once by the executor before {@link #doPart(int, int, int)} is called.
	 *
	 * @param numberOfBatches
	 * 		the number of times {@link #doPart(int, int, int)} will be called.
	 */
	void init(int numberOfBatches);

	/**
	 * @return the total number of operations that can be split into parts.
	 */
	int getNumberOfOperations();

	/**
	 * Does a part of the calculation. This method is called one or more times by the executor and every index
	 * between 0
	 * and {@link #getNumberOfOperations()} is part of exactly one interval {@code [from,to)}.
	 *
	 * @param from
	 * 		the index to start from (inclusive)
	 * @param to
	 * 		the end index (exclusive)
	 * @param batchIndex
	 * 		the index of the part
	 */
	void doPart(int from, int to, int batchIndex);

	/**
	 * Returns the result. Is called from the executor once after all calls to {@link #doPart(int, int, int)} are
	 * finished.
	 *
	 * @return the result
	 */
	T getResult();

}