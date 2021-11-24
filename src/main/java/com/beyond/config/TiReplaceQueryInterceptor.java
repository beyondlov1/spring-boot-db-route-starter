package com.beyond.config;

import com.beyond.tool.DbRouter;
import com.beyond.tool.MergedRoutingSource;
import com.beyond.tool.Range;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Intercepts(
        @Signature(
                type = StatementHandler.class,
                method = "prepare",
                args = {Connection.class, Integer.class}
        )
)
@Slf4j
public class TiReplaceQueryInterceptor implements Interceptor {

    private static final Field boundSqlSqlField;
    private static final Field parameterMappingConfigurationField;

    static {
        try {
            boundSqlSqlField = BoundSql.class.getDeclaredField("sql");
            boundSqlSqlField.setAccessible(true);
            parameterMappingConfigurationField = ParameterMapping.class.getDeclaredField("configuration");
            parameterMappingConfigurationField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<String, List<String>> focusColumns = new HashMap<>();

    static {
        // todo config
        focusColumns.put("db_beyond.order", Arrays.asList("id"));
        focusColumns.put("db_beyond.order_detail", Arrays.asList("order_id"));
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        TiReplaceHolder.RoutingParam routingParam = TiReplaceHolder.get();
        if (routingParam != null && routingParam.getDatabaseType() == TiReplaceHolder.DataSourceType.AUTO && routingParam.isForce()) {
            return autoRoute(invocation);
        }
        if (routingParam != null && routingParam.getDatabaseType() == TiReplaceHolder.DataSourceType.MYSQL && routingParam.isForce()) {
            return noRoute(invocation);
        }
        if (routingParam != null && routingParam.getDatabaseType() == TiReplaceHolder.DataSourceType.TIDB && routingParam.isForce()) {
            return tiRoute(invocation);
        }
        if (TransactionSynchronizationManager.isActualTransactionActive() || !isSelect(invocation)){
            log.debug("transaction is active, no route");
            return noRoute(invocation);
        }
        if (routingParam != null && routingParam.getDatabaseType() == TiReplaceHolder.DataSourceType.AUTO){
            return autoRoute(invocation);
        }
        if (routingParam != null && routingParam.getDatabaseType() == TiReplaceHolder.DataSourceType.MYSQL){
            return noRoute(invocation);
        }
        if (routingParam != null && routingParam.getDatabaseType() == TiReplaceHolder.DataSourceType.TIDB){
            return tiRoute(invocation);
        }
        return autoRoute(invocation);
    }


    private boolean isSelect(Invocation invocation){
        StatementHandler target = (StatementHandler) invocation.getTarget();
        BoundSql boundSql = target.getBoundSql();
        String sql = boundSql.getSql();
        if (StringUtils.isBlank(sql)){
            return false;
        }
        String trim = StringUtils.trim(StringUtils.lowerCase(sql));
        return trim.startsWith("select");
    }


    private Object autoRoute(Invocation invocation) throws InvocationTargetException, IllegalAccessException {
        long t1 = System.currentTimeMillis();

        StatementHandler target = (StatementHandler) invocation.getTarget();
        BoundSql boundSql = target.getBoundSql();
        String sql = boundSql.getSql();

        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();

        try {
            if (containsAny(boundSql.getSql(), focusColumns.keySet())){

                boolean needReplace = false;
                String preparedSql = sql;
                if (CollectionUtils.isNotEmpty(parameterMappings)){
                    ParameterMapping parameterMapping = parameterMappings.get(0);
                    Configuration configuration = (Configuration) parameterMappingConfigurationField.get(parameterMapping);
                    preparedSql = showSql(configuration, boundSql);
                }

                log.debug("intercept sql: {}", preparedSql);

                // 解析判断
                List<MergedRoutingSource> routingSources = DbRouter.parse(preparedSql, focusColumns);
                Set<Integer> focusValues = routingSources.stream().flatMap(x -> x.getValues().stream()).collect(Collectors.toSet());

                //todo route rule
                int dividingPoint = 1312777164;
                for (Integer focusValue : focusValues) {
                    if (focusValue < dividingPoint){
                        needReplace = true;
                        break;
                    }
                }

                if (!needReplace){
                    for (MergedRoutingSource routingSource : routingSources) {
                        List<Range> ranges = routingSource.getRanges();
                        for (Range range : ranges) {
                            if (!range.isValid()){
                                continue;
                            }
                            if (range.include(dividingPoint)){
                                needReplace = true;
                                break;
                            }
                            if (range.getHigh() != null && dividingPoint >= range.getHigh()){
                                needReplace = true;
                                break;
                            }
                        }
                    }
                }

                String replacedSql;
                if (needReplace){
                    replacedSql = boundSql.getSql();
                    for (String focusTable : focusColumns.keySet()) {
                        replacedSql = replaceTiTable(replacedSql, focusTable);
                    }
                    boundSqlSqlField.set(boundSql, replacedSql);
                    log.debug("statement intercept replaced:{}", boundSql.getSql());
                }
            }
        }catch (Exception e){
            log.error("mybatis interceptor sql parse failed, sql:{}", sql, e);
            return invocation.proceed();
        }

        long t2 = System.currentTimeMillis();

        log.debug("routing time: {}", t2-t1);

        return invocation.proceed();
    }

    private Object tiRoute(Invocation invocation) throws InvocationTargetException, IllegalAccessException {
        StatementHandler target = (StatementHandler) invocation.getTarget();
        BoundSql boundSql = target.getBoundSql();
        String sql = boundSql.getSql();

        try {
            String replacedSql = boundSql.getSql();
            for (String focusTable : focusColumns.keySet()) {
                replacedSql = replaceTiTable(replacedSql, focusTable);
            }
            boundSqlSqlField.set(boundSql, replacedSql);
            log.debug("statement intercept replaced:{}", boundSql.getSql());
        } catch (Exception e) {
            log.error("mybatis interceptor sql parse failed, sql:{}", sql, e);
            return invocation.proceed();
        }
        return invocation.proceed();
    }

    private Object noRoute(Invocation invocation) throws InvocationTargetException, IllegalAccessException {
        return invocation.proceed();
    }



    private static String replaceTiTable(String source, String target){
        source = source.replace(target+"\n", target.replace("db_", "ti_")+"\n");
        source = source.replace(target+" ", target.replace("db_", "ti_")+" ");
        return source;
    }

    private static boolean containsAny(String source, Collection<String> list){
        if (StringUtils.isBlank(source)) {
            return false;
        }
        for (String target : list) {
            if (source.contains(target)){
                return true;
            }
        }
        return false;
    }


    // 如果参数是String，则添加单引号， 如果是日期，则转换为时间格式器并加单引号； 对参数是null和不是null的情况作了处理
    private static String getParameterValue(Object obj) {
        String value = null;
        if (obj instanceof String) {
            value = "'" + obj.toString() + "'";
        } else if (obj instanceof Date) {
            DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.DEFAULT,
                    DateFormat.DEFAULT, Locale.CHINA);
            value = "'" + formatter.format(new Date()) + "'";
        } else {
            if (obj != null) {
                value = obj.toString();
            } else {
                value = "";
            }
        }
        return value;
    }

    // 进行？的替换
    public static String showSql(Configuration configuration, BoundSql boundSql) {
        // 获取参数
        Object parameterObject = boundSql.getParameterObject();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        // sql语句中多个空格都用一个空格代替
        String sql = boundSql.getSql().replaceAll("[\\s]+", " ");
        if (CollectionUtils.isNotEmpty(parameterMappings) && parameterObject != null) {
            // 获取类型处理器注册器，类型处理器的功能是进行java类型和数据库类型的转换
            TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
            // 如果根据parameterObject.getClass(）可以找到对应的类型，则替换
            if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
                for (ParameterMapping parameterMapping : parameterMappings) {
                    sql = sql.replaceFirst("\\?",
                            Matcher.quoteReplacement(getParameterValue(parameterObject)));
                }
            } else {
                // MetaObject主要是封装了originalObject对象，提供了get和set的方法用于获取和设置originalObject的属性值,主要支持对JavaBean、Collection、Map三种类型对象的操作
                MetaObject metaObject = configuration.newMetaObject(parameterObject);
                for (ParameterMapping parameterMapping : parameterMappings) {
                    String propertyName = parameterMapping.getProperty();
                    if (metaObject.hasGetter(propertyName)) {
                        Object obj = metaObject.getValue(propertyName);
                        sql = sql.replaceFirst("\\?",
                                Matcher.quoteReplacement(getParameterValue(obj)));
                    } else if (boundSql.hasAdditionalParameter(propertyName)) {
                        // 该分支是动态sql
                        Object obj = boundSql.getAdditionalParameter(propertyName);
                        sql = sql.replaceFirst("\\?",
                                Matcher.quoteReplacement(getParameterValue(obj)));
                    } else {
                        // 打印出缺失，提醒该参数缺失并防止错位
                        sql = sql.replaceFirst("\\?", "缺失");
                    }
                }
            }
        }
        return sql;
    }


    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {

    }
}
