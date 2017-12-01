package io.airlift.stats;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.AtomicDouble;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@ThreadSafe
public class ShardedDecayCounter
{
    private static final int SHARDS = 32;
    private static final int CACHE_SECONDS = 1;

    private final double alpha;
    private final Ticker ticker;
    private final AtomicReference<DecayCounter>[] counters = new AtomicReference[SHARDS];

    private final AtomicLong lastMerge = new AtomicLong();
    private final AtomicDouble mergedCount = new AtomicDouble();
    private DecayCounter merged;


    public ShardedDecayCounter(double alpha)
    {
        this(alpha, Ticker.systemTicker());
    }

    public ShardedDecayCounter(double alpha, Ticker ticker)
    {
        this.alpha = alpha;
        this.ticker = ticker;
        this.merged = new DecayCounter(alpha, ticker);
        for (int i = 0; i < SHARDS; i++) {
            this.counters[i] = new AtomicReference<>();
            this.counters[i].set(new DecayCounter(alpha, ticker));
        }
    }

    public void add(long value)
    {
        counters[(int) (Thread.currentThread().getId() % SHARDS)].get().add(value);
    }

    @Managed
    public synchronized void reset()
    {
        for (int i = 0; i < SHARDS; i++) {
            counters[i].get().reset();
        }
        merged.reset();
        mergedCount.set(0);
    }

    @Managed
    public double getCount()
    {
        long now = TimeUnit.NANOSECONDS.toSeconds(ticker.read());
        if (now - lastMerge.get() > CACHE_SECONDS) {
            synchronized (this) {
                // no need to merge again if another thread happens to finish the merging
                if (now - lastMerge.get() > CACHE_SECONDS) {
                    DecayCounter[] previousCounters = new DecayCounter[SHARDS];
                    for (int i = 0; i < SHARDS; i++) {
                        previousCounters[i] = counters[i].getAndSet(new DecayCounter(alpha, ticker));
                    }
                    for (int i = 0; i < SHARDS; i++) {
                        merged.merge(previousCounters[i]);
                    }
                    lastMerge.set(now);
                    mergedCount.set(merged.getCount());
                }
            }
        }
        return mergedCount.get();
    }

    @Managed
    public double getRate()
    {
        return getCount() * alpha;
    }
}
