package org.apache.flink.streaming.examples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OneThousandSource implements Iterator<String>, Serializable {

    private int currentPosition = 0;

    private final List<Integer> allElements = new ArrayList<>();

    public OneThousandSource() {
        for (int index = 0; index < 1000; ++index) {
            allElements.add(1);
        }
    }

    @Override
    public boolean hasNext() {
        return currentPosition < 1000;
    }

    @Override
    public String next() {
        return String.valueOf(allElements.get(currentPosition++));
    }
}
