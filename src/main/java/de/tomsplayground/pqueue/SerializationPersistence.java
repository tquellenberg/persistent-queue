package de.tomsplayground.pqueue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializationPersistence<E> implements IPersistence<E>{

	private static final Logger log = LoggerFactory.getLogger(SerializationPersistence.class);
	
	public void writeElement(E element, File file) {
		ObjectOutputStream stream = null;
		try {
			stream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
			stream.writeObject(element);
			log.debug("Writing {}", file.getName());
		} catch (IOException e) {
			log.error("Unable to serialize queue element. " + e);
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	@SuppressWarnings("unchecked")
	public E readElement(File file) {
		ObjectInputStream stream = null;
		E element = null;
		try {
			if (file.exists()) {
				stream = new ObjectInputStream(new FileInputStream(file));
				element = (E) stream.readObject();
				log.debug("Reading {}", file.getName());
			} else {
				log.error("Unable to read queue element from {}, discarding.", file.getName());
			}
		} catch (IOException e) {
			log.error("Unable to deserialize queue element, discarding. {} {}", file.getName(), e);
		} catch (ClassNotFoundException e) {
			log.error("Unable to deserialize queue element, discarding. {} {}", file.getName(), e);
		} finally {
			IOUtils.closeQuietly(stream);
		}
		return element;
	}

}
