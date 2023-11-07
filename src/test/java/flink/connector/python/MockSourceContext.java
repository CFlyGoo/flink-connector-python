package flink.connector.python;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.LinkedList;

public class MockSourceContext<T> implements SourceFunction.SourceContext<T> {

    private final LinkedList<T> elements = new LinkedList<>();

    public LinkedList<T> getElements() {
        return elements;
    }

    @Override
    public void collect(T element) {
        this.elements.add(element);
    }

    @Override
    public void collectWithTimestamp(T element, long timestamp) {

    }

    @Override
    public void emitWatermark(Watermark mark) {

    }

    @Override
    public void markAsTemporarilyIdle() {

    }

    @Override
    public Object getCheckpointLock() {
        return null;
    }

    @Override
    public void close() {

    }
}
