package de.tomsplayground.pqueue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a persistent queue. Added elements are immediately serialized and saved 
 * to the file system. Adding and removing elements in parallel is supported.
 * 
 * @TODO Only the basic functions of the queue interface are implemented yet.
 * Creating multiple PersistentQueue with the same directory is not supported and should be prohibit.
 */
public class PersistentQueue<E extends Serializable> implements Queue<E> {

	private static final long POINTER_IS_NOT_INITIALISED = -1L;

	private static final Logger log = LoggerFactory.getLogger(PersistentQueueTest.class);

	/** 
	 * Highest number in queue
	 */
	private final AtomicLong counter = new AtomicLong();
	
	/**
	 * Pointer to last processed element in queue
	 */
	private final AtomicLong pointer = new AtomicLong(POINTER_IS_NOT_INITIALISED);

	/**
	 * Directory where queue elements are stored.
	 */
	private final File queuePath;

	/**
	 * Prefix for file names in queue directory.
	 */
	private final String filenamePrefix;
	
	/**
	 * Persistence of single elements.
	 */
	private final IPersistence<E> persistence = new SerializationPersistence<E>();
	
	public PersistentQueue(File queuePath) {
		this("element", queuePath);
	}
	
	public PersistentQueue(String filenamePrefix, File queuePath) {
		this.filenamePrefix = filenamePrefix;
		this.queuePath = queuePath;
		if (!queuePath.exists()) {
			createQueueDirectory(queuePath);
		} else {
			initialiseCounterAndPointer(queuePath);
		}		
		if (pointer.get() == POINTER_IS_NOT_INITIALISED) {
			pointer.set(0);
		}		
	}
	
	private void initialiseCounterAndPointer(File queuePath) {
		Iterator<File> iterateFiles = FileUtils.iterateFiles(queuePath, new RegexFileFilter("^" + filenamePrefix + "_[0-9]+.obj$"), null);
		while (iterateFiles.hasNext()) {
			File file = iterateFiles.next();
			String fileName = file.getName();
			long queueIndex =  Long.parseLong(StringUtils.substring(fileName, filenamePrefix.length()+1, -4));
			if (queueIndex > counter.get()) {
				counter.set(queueIndex);
			}
			if (queueIndex <= pointer.get() || pointer.get() == POINTER_IS_NOT_INITIALISED) {
				pointer.set(queueIndex - 1);
			}
		}
		log.debug("Pointer {} Counter {}", pointer, counter);
	}

	private void createQueueDirectory(File queuePath) {
		try {
			FileUtils.forceMkdir(queuePath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void clear() {
		synchronized (counter) {
			counter.set(0);
			pointer.set(0);
			try {
				FileUtils.forceDelete(queuePath);
				FileUtils.forceMkdir(queuePath);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
		
	private void putElement(E queueElement) {
		synchronized (counter) {
			long queueIndex = counter.get() + 1;
			String filename = filename(queueIndex);
			File file = new File(queuePath, filename);
			persistence.writeElement(queueElement, file);
			// Jetzt darf Queue Element gelesen werden
			counter.incrementAndGet();
		}
	}

	private String filename(long queueIndex) {
		return filenamePrefix + "_" + queueIndex + ".obj";
	}
	
	private E removeNextQueueElement() {
		synchronized (pointer) {
			if (pointer.get() < counter.get()) {
				long queueIndex = pointer.get()+1;
				String filename = filename(queueIndex);
				File file = new File(queuePath, filename);
				E queueElement = persistence.readElement(file);
				FileUtils.deleteQuietly(file);
				// Jetzt wurde das Queue-Element vollständig gelesen
				pointer.incrementAndGet();
				return queueElement;
			}
		}
		return null;
	}
	
	private E getById(long id) {
		synchronized (pointer) {
			if (id > pointer.get() && id <= counter.get()) {
				String filename = filename(id);
				File file = new File(queuePath, filename);
				return persistence.readElement(file);				
			} else {
				throw new NoSuchElementException("ID: "+id);
			}
		}
	}

	private E getNextQueueElement() {
		synchronized (pointer) {
			if (pointer.get() < counter.get()) {
				long queueIndex = pointer.get()+1;
				String filename = filename(queueIndex);
				File file = new File(queuePath, filename);
				return persistence.readElement(file);
			}
		}
		return null;
	}

	public long getCounter() {
		return counter.get();
	}

	public long getPointer() {
		return pointer.get();
	}
	
	public int size() {
		return (int)(counter.get()-pointer.get());
	}

	public boolean isEmpty() {
		return counter.get() == pointer.get();
	}

	public boolean add(E e) {
		putElement(e);
		return true;
	}

	public boolean offer(E e) {
		putElement(e);
		return true;
	}

	public E remove() {
		E element = removeNextQueueElement();
		if (element == null) {
			throw new NoSuchElementException();
		}
		return element;
	}

	public E poll() {
		return removeNextQueueElement();
	}

	public E element() {
		E element =  getNextQueueElement();
		if (element == null) {
			throw new NoSuchElementException();
		}
		return element;
	}

	public E peek() {
		return getNextQueueElement();
	}

	public boolean addAll(Collection<? extends E> c) {
		for (E e : c) {
			putElement(e);
		}
		return !c.isEmpty();
	}

	public Iterator<E> iterator() {
		return new QueueIterator(counter.get());
	}

	// ====================== Not yet implemented
	
	public boolean contains(Object o) {
		throw new NotImplementedException();
	}

	public Object[] toArray() {
		throw new NotImplementedException();
	}

	public <T> T[] toArray(T[] a) {
		throw new NotImplementedException();
	}

	public boolean remove(Object o) {
		throw new NotImplementedException();
	}

	public boolean containsAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	public boolean removeAll(Collection<?> c) {
		throw new NotImplementedException();
	}

	public boolean retainAll(Collection<?> c) {
		throw new NotImplementedException();
	}
	
	class QueueIterator implements Iterator<E> {
		
		private long id;
		
		QueueIterator(long pointer) {
			this.id = pointer;
		}

		public boolean hasNext() {
			return id > pointer.get();
		}

		public E next() {
			E e = getById(id);
			id--;
			return e;
		}

		public void remove() {
		}
		
	}
}
