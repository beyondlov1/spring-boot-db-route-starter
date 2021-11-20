package com.beyond.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(value={ElementType.METHOD,ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface DataSource {
    TiReplaceHolder.DataSourceType type();

    /**
     * 开启了事务的 "查询" 也强制进行路由, 慎用
     */
    boolean force() default false;

    /**
     * 忽略方法下所有其他 @DataSource 注解
     */
    boolean crash() default false;
}

