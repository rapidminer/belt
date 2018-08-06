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

package com.rapidminer.belt.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.Belt;
import com.rapidminer.belt.Context;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Parameterized.class)
public class TaskRunnerTests {

	private static final Context CTX = Belt.defaultContext();
	private static final String SCHEDULED = "scheduled";
	private static final String SIMPLE = "simple";


	@Parameter
	public String size;

	@Parameters(name = "{0}")
	public static Iterable<String> columnImplementations() {
		return Arrays.asList(SCHEDULED, SIMPLE);
	}

	private <T> TaskRunner<T> makeRunner(Task<T> task) {
		switch (size) {
			case SIMPLE:
				return new SimpleTaskRunner<>(task);
			case SCHEDULED:
				@SuppressWarnings("unchecked")
				ArrayList<Future<T>> futures = new ArrayList<>();
				return new ScheduledTaskRunner<>((c, s) -> {
					futures.add(c.submit(() -> task.call(c, s)));
					return null;
				}, () -> futures.get(0).get());
			default:
				throw new IllegalStateException("Unknown implementation");
		}
	}


	@Test
	public void testContextInteraction() {
		Context ctx = spy(CTX);
		TaskRunner<Integer> task = makeRunner((c, sentinel) -> 42);
		task.run(ctx);
		verify(ctx, atLeast(1)).isActive();
		verify(ctx, times(1)).submit(any());
	}

	@Test
	public void testSimpleReturn() {
		TaskRunner<Integer> task = makeRunner((ctx, sentinel) -> 42);
		assertEquals(42, task.run(CTX).intValue());
	}

	@Test(expected = TaskAbortedException.class)
	public void testInactiveContext() {
		Context ctx = spy(CTX);
		when(ctx.isActive()).thenReturn(false);
		TaskRunner<Void> task = makeRunner((c, sentinel) -> {
			throw new RuntimeException("I am not called");
		});
		task.run(ctx);
	}

	@Test(expected = ExceptionInUserCode.class)
	public void testRuntimeExceptionUnwrapping() {
		TaskRunner<Void> task = makeRunner((ctx, sentinel) -> {
			throw new ExceptionInUserCode();
		});
		task.run(CTX);
	}

	/**
	 * Tests whether interrupting a thread waiting for a task to finish aborts the task and throws the
	 * corresponding exception.
	 *
	 * @throws InterruptedException
	 * 		if the the test thread itself is interrupted
	 */
	@Test(expected = TaskAbortedException.class)
	public void testInterrupt() throws InterruptedException {
		CountDownLatch taskLatch = new CountDownLatch(1);
		CountDownLatch invokerLatch = new CountDownLatch(1);
		CountDownLatch foreverLatch = new CountDownLatch(1);
		RuntimeException[] invokerException = new RuntimeException[1];
		AtomicBoolean[] taskSentinel = new AtomicBoolean[1];

		// Dummy tasks that signals it is running by freeing the task latch
		TaskRunner<Void> task = makeRunner((ctx, sentinel) -> {
			taskSentinel[0] = sentinel;
			taskLatch.countDown();
			try {
				foreverLatch.await();
			} catch (InterruptedException e) {
				// ignore
			}
			return null;
		});

		// Invokes the task, logs the exception, and frees the invoker latch when finished
		Thread invoker = new Thread(() -> {
			try {
				task.run(CTX);
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

		assertFalse(taskSentinel[0].get());
		throw invokerException[0];
	}

	private static class ExceptionInUserCode extends RuntimeException {
		// Placeholder for runtime exception thrown in user code
	}

}


