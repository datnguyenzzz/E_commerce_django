package vn.datnguyen.recommender.Models;

import java.util.List;

public class IndexesCoord {
    private int id; 
    private List<Integer> coord;

    public IndexesCoord(int id, List<Integer> coord) {
        this.id = id ;
        this.coord = coord;
    }

    public int getId() {
        return this.id;
    }

    public List<Integer> getCoord() {
        return this.coord;
    }
}
