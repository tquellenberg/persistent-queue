package de.tomsplayground.pqueue;

import java.io.File;

public interface IPersistence<E> {

	void writeElement(E element, File file);

	E readElement(File file);

}