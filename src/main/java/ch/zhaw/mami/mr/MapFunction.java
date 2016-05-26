package ch.zhaw.mami.mr;

public interface MapFunction<I, O> {

    public O f(I in);
}
