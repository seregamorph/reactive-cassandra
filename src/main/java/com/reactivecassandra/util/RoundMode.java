package com.reactivecassandra.util;

public enum RoundMode {
    UP() {
        @Override
        long round(double value) {
            return Math.round(Math.ceil(value));
        }
    },
    DOWN() {
        @Override
        long round(double value) {
            return Math.round(Math.floor(value));
        }
    },
    ROUND() {
        @Override
        long round(double value) {
            return Math.round(value);
        }
    };

    abstract long round(double value);
}
