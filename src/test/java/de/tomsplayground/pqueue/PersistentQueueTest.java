package de.tomsplayground.pqueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PersistentQueueTest {

	private File queueDir;

	@Before
	public void setUp() {
		queueDir = new File("queue");
	}

	@After
	public void tearDown() {
		FileUtils.deleteQuietly(queueDir);
	}
	
	private Queue<String> createQueue() {
		return new PersistentQueue<String>(queueDir);
//		return new LinkedList<String>();
	}

	@Test
	public void testEmpty() {
		Queue<String> queue = createQueue();

		assertEquals(0, queue.size());
		assertTrue(queue.isEmpty());
	}

	@Test
	public void testRemoveOnEmptyQueue() {
		Queue<String> queue = createQueue();
		try {
			queue.remove();
		} catch (NoSuchElementException e) {
			// Okay
		}
	}
	
	@Test
	public void testElementOnEmptyQueue() {
		Queue<String> queue = createQueue();
		try {
			queue.element();
		} catch (NoSuchElementException e) {
			// Okay
		}
	}
	
	@Test
	public void testPollOnEmptyQueue() {
		Queue<String> queue = createQueue();
		assertNull(queue.poll());
	}
	
	@Test
	public void testPeekOnEmptyQueue() {
		Queue<String> queue = createQueue();
		assertNull(queue.peek());
	}
	
	@Test
	public void testRemoveOnNoneEmptyQueue() {
		Queue<String> queue = createQueue();
		queue.add("one");
		queue.add("two");
		queue.add("three");

		assertEquals("one", queue.remove());
		assertEquals("two", queue.remove());
		assertEquals("three", queue.remove());	
		assertTrue(queue.isEmpty());
	}
	
	@Test
	public void testElementOnNoneEmptyQueue() {
		Queue<String> queue = createQueue();
		queue.add("one");
		queue.add("two");
		queue.add("three");

		assertEquals("one", queue.element());
		assertEquals(3, queue.size());
	}
	
	@Test
	public void testPollOnNoneEmptyQueue() {
		Queue<String> queue = createQueue();
		queue.add("one");
		queue.add("two");
		queue.add("three");

		assertEquals("one", queue.poll());
		assertEquals("two", queue.poll());
		assertEquals("three", queue.poll());	
		assertTrue(queue.isEmpty());
	}
	
	@Test
	public void testPeekOnNoneEmptyQueue() {
		Queue<String> queue = createQueue();
		queue.add("one");
		queue.add("two");
		queue.add("three");

		assertEquals("one", queue.peek());
		assertEquals(3, queue.size());
	}
	
	@Test
	public void testClear() {
		Queue<String> queue = createQueue();
		queue.add("one");
		queue.add("two");
		queue.add("three");

		queue.clear();
		
		assertEquals(0, queue.size());
	}
	
	@Test
	public void testPersistence() throws Exception {
		Queue<String> queue = createQueue();
		queue.add("one");
		queue.add("two");
		queue.add("three");
		queue = null;
		
		Queue<String> queueWithSameDirectory = createQueue();
		assertEquals("one", queueWithSameDirectory.poll());
		assertEquals("two", queueWithSameDirectory.poll());
		assertEquals("three", queueWithSameDirectory.poll());	
	}
}
