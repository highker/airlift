package io.airlift.stats;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(4)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkDecayCounter
{
    private static final int NUMBER_OF_ENTRIES = 100_000;
    private static final int ADD_THREADS = 40;
    private static final int GET_THREADS = 10;

    @State(Scope.Benchmark)
    public static class Counter
    {
        private DecayCounter counter;

        @Setup
        public void setup()
        {
            counter = new DecayCounter(ExponentialDecay.oneMinute());
        }
    }

    @State(Scope.Benchmark)
    public static class ShardedCounter
    {
        private ShardedDecayCounter counter;

        @Setup
        public void setup()
        {
            counter = new ShardedDecayCounter(ExponentialDecay.oneMinute());
        }
    }

    @State(Scope.Thread)
    public static class Data
    {
        private long[] values;

        @Setup
        public void setup()
        {
            values = new long[NUMBER_OF_ENTRIES];
            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                // generate values from a large domain but not many distinct values
                long value = Math.abs((long) (ThreadLocalRandom.current().nextGaussian() * 1_000_000_000));
                values[i] = (value / 1_000_000) * 1_000_000;
            }
        }
    }

    @Benchmark
    @Group("counter")
    @GroupThreads(ADD_THREADS)
    public Object benchmarkAdd(Counter counter, Data data)
    {
        for (long value : data.values) {
            counter.counter.add(value);
        }
        return counter.counter;
    }

    @Benchmark
    @Group("counter")
    @GroupThreads(GET_THREADS)
    public Object benchmarkGet(Counter counter)
    {
        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            counter.counter.getCount();
            counter.counter.getRate();
        }
        return counter.counter;
    }

    @Benchmark
    @Group("shardedCounter")
    @GroupThreads(ADD_THREADS)
    public Object benchmarkShardedAdd(ShardedCounter counter, Data data)
    {
        for (long value : data.values) {
            counter.counter.add(value);
        }
        return counter.counter;
    }

    @Benchmark
    @Group("shardedCounter")
    @GroupThreads(GET_THREADS)
    public Object benchmarkShardedGet(ShardedCounter counter)
    {
        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            counter.counter.getCount();
            counter.counter.getRate();
        }
        return counter.counter;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkDecayCounter.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
