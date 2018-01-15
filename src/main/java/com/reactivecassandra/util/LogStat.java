package com.reactivecassandra.util;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import javax.annotation.concurrent.ThreadSafe;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Logarithmic statistics
 * <pre>
 * where M is mantissa (default 1.0), B is base (default sqrt(2.0)),
 * N = round(log_B_(value/M))
 * (the origin formula is M*B^N=value)
 * N is the rounded to integer depending on RoundMode.
 * </pre>
 * The key for stat grouping is M*B^N.
 */
@Beta
@ThreadSafe
public class LogStat {
    private static final double DEFAULT_MANTISSA = 1.0d;
    private static final double DEFAULT_BASE = Math.sqrt(2.0d);
    private static final RoundMode DEFAULT_ROUND_MODE = RoundMode.ROUND;
    private static final Locale DEFAULT_LOCALE = Locale.ENGLISH;

    private final ConcurrentMap<Long, AtomicLong> stat = Maps.newConcurrentMap();

    private final double mantissa;
    private final double base;
    private final double logBase;
    private final RoundMode roundMode;

    public LogStat() {
        this(DEFAULT_ROUND_MODE);
    }

    public LogStat(RoundMode roundMode) {
        this(DEFAULT_BASE, roundMode);
    }

    public LogStat(double base, RoundMode roundMode) {
        this(DEFAULT_MANTISSA, base, roundMode);
    }

    public LogStat(double mantissa, double base, RoundMode roundMode) {
        Preconditions.checkArgument(mantissa > 0.0d, "Illegal base value %s", base);
        Preconditions.checkArgument(base > 1.0d, "Illegal base value %s", base);
        Preconditions.checkNotNull(roundMode, "roundMode is null");

        this.mantissa = mantissa;
        this.base = base;
        this.logBase = Math.log(base);
        this.roundMode = roundMode;
    }

    public double mantissa() {
        return mantissa;
    }

    public double base() {
        return base;
    }

    public RoundMode roundMode() {
        return roundMode;
    }

    public void addValue(double value) {
        Preconditions.checkArgument(value >= 0.0d, "Illegal value %s", value);
        long key;
        if (value == 0.0d) {
            // special case
            key = Long.MIN_VALUE;
        } else {
            key = roundMode.round(Math.log(value / this.mantissa) / this.logBase);
        }
        stat.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    public SortedMap<Double, Long> getStat() {
        return stat.entrySet().stream().collect(CollectUtils.toTreeMap(
                e -> this.mantissa * Math.pow(this.base, e.getKey()),
                e -> e.getValue().get()
        ));
    }

    /**
     * Get formatted report. The double values are formatted with precision,
     * sorted ascending as double values.
     *
     * @return
     */
    public Map<String, Long> getReportStat(int precision) {
        Preconditions.checkArgument(precision >= 0, "Illegal precision %s", precision);

        return getStat().entrySet().stream().collect(Collectors.toMap(
                e -> format(e.getKey(), precision),
                Map.Entry::getValue,
                Long::sum,
                LinkedHashMap::new
        ));
    }

    private static String format(double value, int precision) {
        return String.format(DEFAULT_LOCALE, "%." + precision + "f", value);
    }

    @Override
    public String toString() {
        return "LogStat{" +
                "stat=" + stat +
                ", base=" + base +
                ", logBase=" + logBase +
                ", mantissa=" + mantissa +
                ", roundMode=" + roundMode +
                '}';
    }
}
