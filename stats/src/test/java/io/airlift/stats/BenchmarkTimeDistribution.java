package io.airlift.stats;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class BenchmarkTimeDistribution
{
    private static final int NUMBER_OF_ENTRIES = 1_000;

    @State(Scope.Benchmark)
    public static class Stats
    {
        private TimeDistribution distribution;

        @Setup
        public void setup()
        {
            distribution = new TimeDistribution();
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
    @Threads(1024)
    public Object benchmarkAdd(Stats stats, Data data)
    {
        for (long value : data.values) {
            stats.distribution.add(value);
        }
        return stats.distribution.getPercentiles();
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    @Threads(1024)
    public Object benchmarkGetAndAdd(Stats stats, Data data)
    {
        for (long value : data.values) {
            stats.distribution.add(value);
            stats.distribution.getMax();
            stats.distribution.getMin();
            stats.distribution.getP50();
            stats.distribution.getP75();
            stats.distribution.getP90();
            stats.distribution.getCount();
            stats.distribution.getMaxError();
        }
        return stats.distribution.getPercentiles();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkTimeDistribution.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
