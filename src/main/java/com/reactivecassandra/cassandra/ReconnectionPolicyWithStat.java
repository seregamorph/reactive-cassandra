package com.reactivecassandra.cassandra;

import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ReconnectionPolicyWithStat implements ReconnectionPolicy, ReportProvider {
    private final ReconnectionPolicy delegate;

    private final AtomicInteger newScheduleCounter = new AtomicInteger();

    public ReconnectionPolicyWithStat(ReconnectionPolicy delegate) {
        this.delegate = Preconditions.checkNotNull(delegate);
    }

    protected final ReconnectionPolicy delegate() {
        return delegate;
    }

    @Override
    public ReconnectionSchedule newSchedule() {
        newScheduleCounter.incrementAndGet();
        return delegate().newSchedule();
    }

    @Override
    public List<Map<String, Object>> getReport(Map<String, String> args) {
        return Arrays.asList(
                ImmutableMap.of("delegate", String.valueOf(delegate())),
                ImmutableMap.of("newScheduleCounter", newScheduleCounter.get())
        );
    }

    @Override
    public String toString() {
        return "ReconnectionPolicyWithStat{" +
                "delegate=" + delegate +
                ", newScheduleCounter=" + newScheduleCounter +
                '}';
    }
}
