/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionAbortedException;
import com.rapidminer.belt.execution.ExecutionUtils;


/**
 * @author Michael Knopf, Gisa Meier
 */
public class ExecutionUtilsTests {

	private static final Context CTX = Belt.defaultContext();

	@Test
	public void testContextInteraction() throws ExecutionException {
		Context ctx = spy(CTX);
		ExecutionUtils.run(() -> 42, ctx);
		verify(ctx, atLeast(1)).isActive();
		verify(ctx, times(1)).call(any());
	}

	@Test
	public void testSimpleReturn() {
		Integer result = ExecutionUtils.run(()->42, CTX);
		assertEquals(42, result.intValue());
	}

	@Test(expected = ExecutionAbortedException.class)
	public void testInactiveContext() {
		Context ctx = spy(CTX);
		when(ctx.isActive()).thenReturn(false);
		Callable<Void> task = () -> {
			throw new RuntimeException("I am not called");
		};
		ExecutionUtils.run(task, ctx);
	}

	@Test(expected = ExceptionInUserCode.class)
	public void testRuntimeExceptionUnwrapping() {
		Callable<Void> task = () -> {
			throw new ExceptionInUserCode();
		};
		ExecutionUtils.run(task, CTX);
	}

	/**
	 * Tests whether interrupting a thread waiting for a task to finish aborts the task and throws the
	 * corresponding exception.
	 *
	 * @throws InterruptedException
	 * 		if the the test thread itself is interrupted
	 */
	@Test(expected = ExecutionAbortedException.class)
	public void testInterrupt() throws InterruptedException {
		CountDownLatch taskLatch = new CountDownLatch(1);
		CountDownLatch invokerLatch = new CountDownLatch(1);
		CountDownLatch foreverLatch = new CountDownLatch(1);
		RuntimeException[] invokerException = new RuntimeException[1];
		// Dummy tasks that signals it is running by freeing the task latch
		Callable<Void> task = () -> {
			taskLatch.countDown();
			try {
				foreverLatch.await();
			} catch (InterruptedException e) {
				// ignore
			}
			return null;
		};

		// Invokes the task, logs the exception, and frees the invoker latch when finished
		Thread invoker = new Thread(() -> {
			try {
				ExecutionUtils.run(task, CTX);
			} catch (RuntimeException e) {
				invokerException[0] = e;
			} finally {
				invokerLatch.countDown();
			}
		});

		// Interrupts the invoker thread as soon as the task has started running
		Thread interrupter = new Thread(() -> {
			try {
				// Interrupt thread running the task as soon as the latch is freed.
				taskLatch.await();
				invoker.interrupt();
			} catch (InterruptedException e) {
				// ignore
			}

		});

		interrupter.start();
		invoker.start();
		invokerLatch.await();

		throw invokerException[0];
	}

	private static class ExceptionInUserCode extends RuntimeException {
		// Placeholder for runtime exception thrown in user code
	}

}


