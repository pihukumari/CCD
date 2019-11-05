package com.ccd.model;

import java.util.concurrent.atomic.AtomicLong;

public class SequenceGenerator implements SequenceGeneratorInterface {
	    private AtomicLong value = new AtomicLong(1);

	    @Override
	    public long getNext() {
	        return value.getAndIncrement();
	    }
	

}
