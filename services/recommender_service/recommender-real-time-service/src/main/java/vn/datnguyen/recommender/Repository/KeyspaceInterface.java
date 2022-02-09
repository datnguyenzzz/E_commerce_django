package vn.datnguyen.recommender.Repository;

public interface KeyspaceInterface {
    /**
     *  @param KeyspaceName
     *  @param NumberOfNodeReplicas
     */
    public void createAndUseKeyspace(String name, int numReplicas);
}
