package org.bobstuff.bongo;

import java.util.ArrayList;
import java.util.List;

public class BongoIndexMap {
    private List<Integer> indexes = new ArrayList<>();

    public void add(int index) {
        indexes.add(index);
    }

    public int get(int index) {
        return indexes.get(index);
    }
}
