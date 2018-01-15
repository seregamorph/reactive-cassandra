package com.reactivecassandra.cassandra;

import java.util.List;

public abstract class BindArgs {
    BindArgs() {
    }

    /**
     * Bind argument value as marker. May be collection (but not for IN clause).
     *
     * @param value
     * @return
     */
    public abstract Object bind(Object value);

    /**
     * Bind list argument value. Note: used only for IN clause
     *
     * @param values values for IN clause
     * @return
     */
    public abstract List<Object> bindAll(List<?> values);
}
