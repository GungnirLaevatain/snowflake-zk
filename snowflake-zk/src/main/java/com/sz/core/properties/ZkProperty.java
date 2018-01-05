package com.sz.core.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The class Zk property.
 *
 * @author GungnirLaevatain
 * @version 2018 -01-04 16:06:02
 * @since JDK 1.8
 */
@ConfigurationProperties(prefix = ZkProperty.PREFIX)
@Data
public class ZkProperty {

    public static final String PREFIX = "sz.zk.config";

    /**
     * The Name space.
     * zk根目录名称
     */
    private String nameSpace = "snowflake";
    /**
     * The Zk service.
     * 以,隔开的ip:port集合
     */
    private String zkService = "127.0.0.1:2181";
    /**
     * The Max retry.
     * 最大重试次数
     */
    private int maxRetry = 6;
    /**
     * The Sleep ms between retries.
     * 每次重试的间隔时间，单位为ms
     */
    private int sleepMsBetweenRetries = 1000;

    /**
     * The Session timeout ms.
     * session超时时间
     */
    private int sessionTimeoutMs = 3000;

    /**
     * The Connection timeout ms.
     * 连接超时时间
     */
    private int connectionTimeoutMs = 1000;
}
