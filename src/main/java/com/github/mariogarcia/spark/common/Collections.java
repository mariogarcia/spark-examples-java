package github.mariogarcia.spark.common;

import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Collections related functions
 *
 * @since 0.1.0
 */
public final class Collections {

    private Collections() {
        // NOTHING
    }

    /**
     * Converts a given {@link Iterator} to a {@link Stream} of the
     * same type
     *
     * @param iterator the souce {@link Iterator}
     * @return an instance of {@link Stream}
     * @since 0.1.0
     */
    public static  <T> Stream<T> toStream(Iterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;

        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static <T extends Integer> Integer sum(T left, T right) {
        return left + right;
    }
}
