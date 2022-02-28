package vn.datnguyen.recommender.Models;

import java.util.List;

public class IndexesCoord {
    private int id; 
    private List<Integer> coord, upperBoundRange;

    public IndexesCoord(int id, List<Integer> coord, List<Integer> upperBoundRange) {
        this.id = id ;
        this.coord = coord;
        this.upperBoundRange = upperBoundRange;
    }

    public int getId() {
        return this.id;
    }

    public List<Integer> getCoord() {
        return this.coord;
    }

    public List<Integer> getUpperBoundRange() {
        return this.upperBoundRange;
    }
}
