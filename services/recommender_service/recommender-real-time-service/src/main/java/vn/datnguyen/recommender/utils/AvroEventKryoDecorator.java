package vn.datnguyen.recommender.utils;

import com.esotericsoftware.kryo.Kryo;

import org.apache.storm.serialization.IKryoDecorator;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public class AvroEventKryoDecorator implements IKryoDecorator {
    
    @Override
    public void decorate(Kryo kryo) {
        kryo.register(AvroEvent.class, new AvroEventSerializer());
    }
}
