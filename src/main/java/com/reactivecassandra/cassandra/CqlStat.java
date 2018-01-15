package com.reactivecassandra.cassandra;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.reactivecassandra.util.AvgValue;
import com.reactivecassandra.util.LimitedLinkedHashMap;
import com.reactivecassandra.util.LogStat;
import com.reactivecassandra.util.RoundMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;

/**
 * CQL execution statistics
 */
@ThreadSafe
class CqlStat {
    private static final double AVG_FACTOR = 0.2d;
    private static final int MAP_SIZE = 256;
    /**
     * Multiplier constant to convert nanos to millis
     */
    private static final double NANOS_TO_MILLIS_MULTIPLIER = 0.000001d;

    private final long startNanos = System.nanoTime();
    private final Map<String, CqlStatementStat> queryMap = LimitedLinkedHashMap.create(MAP_SIZE);

    private long totalPrepareCount;
    private long totalExecuteCount;
    private long totalExecuteTimeNanos;

    CqlStat() {
    }

    synchronized List<Map<String, Object>> report(@Nullable String sortColumnName) {
        long uptimeSeconds = TimeUnit.MILLISECONDS.toSeconds(getUptimeMs());

        List<Map<String, Object>> res = Lists.newArrayList();

        res.add(ImmutableMap.of(
                "totalExecuteCount", totalExecuteCount,
                "totalPrepareCount", totalPrepareCount,
                "totalExecuteTimeMs", TimeUnit.NANOSECONDS.toMillis(totalExecuteTimeNanos),
                "avgActive", getAvgActive()
        ));

        List<CqlStatementStat> statsToSort = Lists.newArrayList(queryMap.values());
        statsToSort.sort(getComparator(sortColumnName));

        for (CqlStatementStat statEntry : statsToSort) {
            Map<String, Object> row = statEntry.getStat(uptimeSeconds);
            res.add(row);
        }

        return res;
    }

    private static Comparator<CqlStatementStat> getComparator(@Nullable String sort) {
        if ("query".equals(sort)) {
            return Comparator.comparing(e -> e.query, String.CASE_INSENSITIVE_ORDER);
        } else if ("totalExecuteTime".equals(sort)) {
            return Comparator.comparing(e -> e.totalExecuteTimeNanos, reverseOrder());
        } else if ("executeAvgTotalTime".equals(sort)) {
            return Comparator.comparing(e -> avg(e.totalExecuteTimeNanos, e.executeCount), reverseOrder());
        } else if ("maxExecuteTime".equals(sort)) {
            return Comparator.comparing(e -> e.maxExecuteTimeMs, reverseOrder());
        }
        return Comparator.comparing(e -> e.executeCount, reverseOrder());
    }

    synchronized void registerPrepare(String query, @Nullable Throwable exception) {
        totalPrepareCount++;
        getCqlStat(query).registerPrepare(exception);
    }

    synchronized void registerExecute(String query, @Nullable Object[] args, long timeNanos, @Nullable Throwable exception) {
        this.totalExecuteCount++;
        this.totalExecuteTimeNanos += timeNanos;
        getCqlStat(query).registerExecute(args, timeNanos, exception);
    }

//    synchronized void registerFetch(String query, long time) {
//        getCqlStat(query)
//                .incFetchTime(time);
//    }

    synchronized void registerBatchSize(String query, int batchSize) {
        getCqlStat(query).incBatchCount(batchSize);
    }

    private String getAvgActive() {
        long totalHoldTime = TimeUnit.NANOSECONDS.toMillis(totalExecuteTimeNanos);
        return formatAvg(totalHoldTime, getUptimeMs(), 1.0d);
    }

    synchronized void clear() {
        queryMap.clear();
    }

    private static double avg(long total, long count) {
        if (count == 0L) {
            return 0.0d;
        }
        return ((double) total) / count;
    }

    static String formatAvg(long total, long count, double multiplier) {
        double value = avg(total, count) * multiplier;
        return String.format(Locale.ENGLISH, "%.2f", value);
    }

    private long getUptimeMs() {
        long uptime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        return uptime == 0 ? 1000L : uptime;
    }

    @Nonnull
    private CqlStatementStat getCqlStat(@Nullable String query) {
        assert Thread.holdsLock(this);

        return queryMap.computeIfAbsent(Objects.firstNonNull(query, "[null]"), CqlStatementStat::new);
    }

    @ThreadSafe
    private static class ExceptionStat {
        private int count;
        @Nullable
        private String stackTrace;

        private ExceptionStat() {
        }

        int getCount() {
            return count;
        }

        @Nullable
        String getStackTrace() {
            return stackTrace;
        }

        void setStackTrace(String stackTrace) {
            this.stackTrace = stackTrace;
        }

        void incCount() {
            count++;
        }

        @Override
        public String toString() {
            return "ExceptionStat{" +
                    "count=" + count +
                    ", stackTrace=" + stackTrace +
                    '}';
        }
    }

    @NotThreadSafe
    private static class ArgsStat {
        final String argsf;

        long executeCount;
        long totalExecuteTimeNanos;

        private ArgsStat(String argsf) {
            this.argsf = argsf;
        }

        void incExecuteCount(long timeNanos) {
            executeCount++;
            totalExecuteTimeNanos += timeNanos;
        }
    }

    private static final double BASE = Math.sqrt(2.0d);

    @NotThreadSafe
    private static class CqlStatementStat {
        final String query;

        private final Map<String, ArgsStat> argsMap = LimitedLinkedHashMap.create(MAP_SIZE);

        long prepareCount;
        long executeCount;
        long totalExecuteTimeNanos;
        long maxExecuteTimeMs;
        //        long totalFetchTime;
        final AvgValue avgExecuteTimeMs;
        final LogStat executeStat = new LogStat(BASE, RoundMode.ROUND);
        //        final AvgValue avgFetchTime;
        long totalBatchCount;
        long failPrepareCount;
        long failExecuteCount;
        /**
         * Exception string -> stat
         */
        private Map<String, ExceptionStat> exceptions;

        CqlStatementStat(String query) {
            this.query = Preconditions.checkNotNull(query);
            this.exceptions = LimitedLinkedHashMap.create(5);
            this.avgExecuteTimeMs = new AvgValue(AVG_FACTOR);
//            this.avgFetchTime = new AvgValue(AVG_FACTOR);
        }

//        void incFetchTime(long time) {
//            totalFetchTime += time;
//            avgFetchTime.update((double) time);
//        }

        void registerPrepare(@Nullable Throwable exception) {
            prepareCount++;
            if (exception != null) {
                failPrepareCount++;
                registerException(exception);
            }
        }

        void registerExecute(@Nullable Object[] args, long timeNanos, @Nullable Throwable exception) {
            executeCount++;
            totalExecuteTimeNanos += timeNanos;
            double timeMs = timeNanos * NANOS_TO_MILLIS_MULTIPLIER;
            avgExecuteTimeMs.update(timeMs);
            executeStat.addValue(timeMs);
            maxExecuteTimeMs = Math.max(maxExecuteTimeMs, TimeUnit.NANOSECONDS.toMillis(timeNanos));

            String argsf = formatArgs(args);
            ArgsStat argsStat = argsMap.computeIfAbsent(argsf, ArgsStat::new);
            argsStat.incExecuteCount(timeNanos);

            if (exception != null) {
                failExecuteCount++;
                registerException(exception);
            }
        }

        void incBatchCount(int batchSize) {
            totalBatchCount += batchSize;
        }

        void registerException(@Nonnull Throwable exception) {
            String key = exception.toString();
            ExceptionStat exceptionStat = exceptions.computeIfAbsent(key, v -> new ExceptionStat());
            exceptionStat.incCount();

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);
            exceptionStat.setStackTrace(sw.toString());
        }

        Map<String, Object> getStat(long uptimeSeconds) {
            Map<String, Object> map = Maps.newLinkedHashMap();

            map.put("query", query);

            map.put("prepareCount", prepareCount);
            map.put("executeCount", executeCount);
            map.put("execPerMinute", formatAvg(executeCount, uptimeSeconds, 60));

            if (failPrepareCount != 0) {
                map.put("failPrepareCount", failPrepareCount);
            }
            if (failExecuteCount != 0) {
                map.put("failExecuteCount", failExecuteCount);
            }

            map.put("totalExecuteTime", TimeUnit.NANOSECONDS.toMillis(totalExecuteTimeNanos));
            map.put("maxExecuteTime", maxExecuteTimeMs);
            map.put("executeAvgTotalTime", formatAvg(totalExecuteTimeNanos, executeCount, NANOS_TO_MILLIS_MULTIPLIER));
            map.put("executeAvgFloatTime", avgExecuteTimeMs.toString(2));
            map.put("executeStat", executeStat.getReportStat(2));

//            if (totalFetchTime > 0) {
//                map.put("totalFetchTime", totalFetchTime);
//                map.put("fetchAvgTotalTime", formatAvg(totalFetchTime, executeCount, 1));
//                map.put("fetchAvgFloatTime", avgFetchTime.toString(2));
//            }

            if (totalBatchCount > 0) {
                map.put("totalBatchCount", totalBatchCount);
                map.put("avgBatchSize", formatAvg(totalBatchCount, executeCount, 1));
            }

            List<ArgsStat> argsStats = Lists.newArrayList(argsMap.values());
            argsStats.sort(Comparator.comparing(e -> e.executeCount, Comparator.reverseOrder()));

            map.put("args", argsStats.stream().map(as -> ImmutableMap.of(
                    "format", as.argsf,
                    "executeCount", as.executeCount,
                    "executeAvgTotalTime", formatAvg(as.totalExecuteTimeNanos, as.executeCount, NANOS_TO_MILLIS_MULTIPLIER)
            )).collect(Collectors.toList()));

            List<Map<String, Object>> exList = Lists.newArrayList();
            for (Map.Entry<String, ExceptionStat> entry : exceptions.entrySet()) {
                ExceptionStat exceptionStat = entry.getValue();

                Map<String, Object> exMap = Maps.newLinkedHashMap();
                exMap.put("message", entry.getKey());
                exMap.put("count", exceptionStat.getCount());
                String stackTrace = exceptionStat.getStackTrace();
                if (stackTrace != null) {
                    exMap.put("stackTrace", stackTrace);
                }
                exList.add(exMap);
            }
            if (!exList.isEmpty()) {
                map.put("exceptions", exList);
            }

            return map;
        }

        @Nonnull
        static String formatArgs(@Nullable Object[] args) {
            if (args == null || args.length == 0) {
                return "[]";
            }
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                if (i > 0) {
                    sb.append(", ");
                }
                if (arg == null) {
                    sb.append("null");
                } else if (arg instanceof Set) {
                    sb.append("{").append(((Set<?>) arg).size()).append("}");
                } else if (arg instanceof Map) {
                    sb.append("{").append(((Map<?, ?>) arg).size()).append("}");
                } else if (arg instanceof Collection) {
                    sb.append("[").append(((Collection<?>) arg).size()).append("]");
                } else {
                    sb.append("?");
                }
            }
            return sb.append("]").toString();
        }

        @Override
        public String toString() {
            return "CqlStatementStat{" +
                    "query='" + query + '\'' +
                    ", argsMap=" + argsMap +
                    ", prepareCount=" + prepareCount +
                    ", executeCount=" + executeCount +
                    ", totalExecuteTimeNanos=" + totalExecuteTimeNanos +
                    ", maxExecuteTimeMs=" + maxExecuteTimeMs +
                    ", avgExecuteTimeMs=" + avgExecuteTimeMs +
                    ", totalBatchCount=" + totalBatchCount +
                    ", failPrepareCount=" + failPrepareCount +
                    ", failExecuteCount=" + failExecuteCount +
                    ", exceptions=" + exceptions +
                    '}';
        }
    }
}
