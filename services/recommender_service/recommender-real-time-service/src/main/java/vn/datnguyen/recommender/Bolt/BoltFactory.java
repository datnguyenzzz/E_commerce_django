package vn.datnguyen.recommender.Bolt;

public class BoltFactory {
    
    public BoltFactory() {}

    public LoggerBolt creatLoggerBolt() {
        return new LoggerBolt();
    }

    public WeightApplierBolt createWeightApplierBolt() {
        return new WeightApplierBolt();
    }
}
