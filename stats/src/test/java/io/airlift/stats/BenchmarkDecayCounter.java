package io.airlift.stats;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkDecayCounter
{
    private static final int NUMBER_OF_ENTRIES = 100_000;

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
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    @Group("counter")
    @GroupThreads(40)
    public Object benchmarkAdd(Counter counter, Data data)
    {
        for (long value : data.values) {
            counter.counter.add(value);
        }
        return counter.counter;
    }

    @Benchmark
    @Group("counter")
    @GroupThreads(5)
    public Object benchmarkGet(Counter counter)
    {
        counter.counter.getCount();
        return counter.counter.getRate();
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    @Group("shardedCounter")
    @GroupThreads(40)
    public Object benchmarkShardedAdd(ShardedCounter counter, Data data)
    {
        for (long value : data.values) {
            counter.counter.add(value);
        }
        return counter.counter;
    }

    @Benchmark
    @Group("shardedCounter")
    @GroupThreads(5)
    public Object benchmarkShardedGet(ShardedCounter counter)
    {
        counter.counter.getCount();
        return counter.counter.getRate();
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
