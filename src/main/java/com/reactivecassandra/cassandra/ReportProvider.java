package com.reactivecassandra.cassandra;

import java.util.Map;

public interface ReportProvider {
    /**
     * Get report
     *
     * @param args
     * @return json-serializable report
     */
    Object getReport(Map<String, String> args);
}
