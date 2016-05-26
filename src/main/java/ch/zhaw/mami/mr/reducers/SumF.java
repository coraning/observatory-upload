package ch.zhaw.mami.mr.reducers;

import ch.zhaw.mami.mr.ReduceFunction;

public class SumF implements ReduceFunction<Long> {

    public Long f(final Long a, final Long b) {
        return a + b;
    }
}
