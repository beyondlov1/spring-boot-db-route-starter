package com.beyond.config;

import java.util.Stack;

/**
 * @author chenshipeng
 * @date 2021/11/10
 */
public class TiReplaceHolder {

    private static final ThreadLocal<Stack<RoutingParam>> instance = new ThreadLocal<>();

    public static void push(String id, DataSourceType dataSourceType,  boolean force, boolean crash) {
        Stack<RoutingParam> stack = instance.get();
        if (stack == null) {
            stack = new Stack<>();
        }
        stack.push(new RoutingParam(id, dataSourceType, force, crash));
        instance.set(stack);
    }

    public static RoutingParam pop() {
        if (instance.get() == null || instance.get().size() == 0) {
            return null;
        }
        return instance.get().pop();
    }

    public static RoutingParam get() {
        if (instance.get() == null || instance.get().size() == 0) {
            return null;
        }
        return instance.get().peek();
    }

    public static class RoutingParam {
        private final String id;
        private final DataSourceType dataSourceType;
        private final boolean force;
        private final boolean crash;

        public RoutingParam(String id, DataSourceType dataSourceType) {
            this(id, dataSourceType, false, false);
        }

        public RoutingParam(String id, DataSourceType dataSourceType,  boolean force, boolean crash) {
            this.id = id;
            this.dataSourceType = dataSourceType;
            this.force = force;
            this.crash = crash;
        }

        public String getId() {
            return id;
        }

        public DataSourceType getDatabaseType() {
            return dataSourceType;
        }


        public boolean isForce() {
            return force;
        }

        public DataSourceType getDataSourceType() {
            return dataSourceType;
        }

        public boolean isCrash() {
            return crash;
        }

    }

    public enum DataSourceType {
        AUTO, MYSQL, TIDB
    }
}
