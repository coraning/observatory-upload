package ch.zhaw.mami.mr.mappers;

import org.apache.hadoop.io.BytesWritable;

import ch.zhaw.mami.mr.MapFunction;

public class SizeF implements MapFunction<BytesWritable, Long> {

    @Override
    public Long f(final BytesWritable value) {

        return (long) value.getLength();
    }
}
