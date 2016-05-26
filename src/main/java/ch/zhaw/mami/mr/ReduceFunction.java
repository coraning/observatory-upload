package ch.zhaw.mami.mr;

public interface ReduceFunction<R> {

    public R f(R a, R b);
}
