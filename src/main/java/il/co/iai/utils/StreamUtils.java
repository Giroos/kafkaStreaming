package il.co.iai.utils;

import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {
  public static <A, B> Stream<Tuple2<A, B>> zip(Stream<A> first, Stream<B> second) {
    ZipIterator<A, B> iterator = new ZipIterator<>(first.iterator(), second.iterator());
    return StreamSupport.stream(
      Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
      false);
  }

  public static <A, B, C> Stream<Tuple3<A, B, C>> zip(Stream<A> first, Stream<B> second, Stream<C> third) {
    Zip3Iterator<A, B, C> iterator = new Zip3Iterator<>(first.iterator(), second.iterator(), third.iterator());
    return StreamSupport.stream(
      Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
      false);
  }

  public static <A> Optional<Integer> indexOf(List<A> list, Predicate<A> predicate) {
    int idx = 0;
    for (A item: list) {
      if (predicate.test(item)) {
        return Optional.of(idx);
      }
      idx++;
    }
    return Optional.empty();
  }

  public static class ZipIterator<A, B> implements Iterator<Tuple2<A, B>> {
    Iterator<A> firstIterator;
    Iterator<B> secondIterator;

    ZipIterator(Iterator<A> firstIterator, Iterator<B> secondIterator) {
      this.firstIterator = firstIterator;
      this.secondIterator = secondIterator;
    }


    @Override
    public boolean hasNext() {
      return firstIterator.hasNext() && secondIterator.hasNext();
    }

    @Override
    public Tuple2<A, B> next() {
      return new Tuple2<>(firstIterator.next(), secondIterator.next());
    }
  }

  public static class Zip3Iterator<A, B, C> implements Iterator<Tuple3<A, B, C>> {
    ZipIterator<Tuple2<A, B>, C> sourceIterator;

    Zip3Iterator(ZipIterator<Tuple2<A, B>, C> sourceIterator) {
      this.sourceIterator = sourceIterator;
    }

    Zip3Iterator(Iterator<A> first, Iterator<B> second, Iterator<C> third) {
      this.sourceIterator = new ZipIterator<>(
        new ZipIterator<>(first, second),
        third
      );
    }

    @Override
    public boolean hasNext() {
      return sourceIterator.hasNext();
    }

    @Override
    public Tuple3<A, B, C> next() {
      Tuple2<Tuple2<A, B>, C> elem = sourceIterator.next();
      return new Tuple3<>(elem._1._1, elem._1._2, elem._2);
    }
  }
}
