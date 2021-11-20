package com.beyond.config;

import org.apache.ibatis.session.SqlSessionFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author chenshipeng
 * @date 2021/11/05
 */
@Configuration
@ConditionalOnMissingClass(value = {"com.github.pagehelper.autoconfigure.PageHelperAutoConfiguration"})
@Aspect
public class MyBatisConfigMissing {

    @Autowired
    private List<SqlSessionFactory> sqlSessionFactoryList;

    @PostConstruct
    public void addMyInterceptor() {
        for (SqlSessionFactory sqlSessionFactory : sqlSessionFactoryList) {
            org.apache.ibatis.session.Configuration configuration = sqlSessionFactory.getConfiguration();
            configuration.addInterceptor(new TiReplaceQueryInterceptor());
        }
    }

    @Pointcut("@annotation(com.beyond.config.DataSource)")
    public void dataSourcePointcut(){}

    /**
     * 拦截器具体实现
     */
    @Around("dataSourcePointcut()")
    public Object intercept(ProceedingJoinPoint pjp) throws Throwable {
        MethodSignature signature = (MethodSignature) pjp.getSignature();
        DataSource dataSourceOnMethod = signature.getMethod().getAnnotation(DataSource.class);
        TiReplaceHolder.DataSourceType type = dataSourceOnMethod.type();
        TiReplaceHolder.RoutingParam routingParam = TiReplaceHolder.get();
        if (routingParam != null && routingParam.isCrash()){
            return pjp.proceed(pjp.getArgs());
        }
        TiReplaceHolder.push(signature.getMethod().getDeclaringClass().getName() + "."+signature.getMethod().getName(),
                type, dataSourceOnMethod.force(), dataSourceOnMethod.crash());
        Object result = pjp.proceed(pjp.getArgs());
        TiReplaceHolder.pop();
        return result;
    }
}
