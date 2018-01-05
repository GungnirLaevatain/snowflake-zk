package com.sz.core.autoconfigure;

import com.sz.core.properties.ZkProperty;
import com.sz.core.utils.SnowflakeIdWorker;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The class SZ config.
 * 利用zk给id生成器分配workId
 *
 * @author GungnirLaevatain
 * @version 2018 -01-04 15:42:34
 * @since JDK 1.8
 */
@EnableConfigurationProperties(ZkProperty.class)
@Configuration
public class SZConfig {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ZkProperty zkProperty;

    @Autowired
    public SZConfig(ZkProperty zkProperty) {
        this.zkProperty = zkProperty;
    }

    /**
     * Create zk client.
     * 构建zk客户端
     *
     * @return the curator framework
     */
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    public CuratorFramework createZkClient() {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(zkProperty.getSleepMsBetweenRetries(), zkProperty.getMaxRetry());
        return CuratorFrameworkFactory.builder()
                .namespace(zkProperty.getNameSpace())
                .sessionTimeoutMs(zkProperty.getSessionTimeoutMs())
                .connectionTimeoutMs(zkProperty.getConnectionTimeoutMs())
                .retryPolicy(retryPolicy)
                .connectString(zkProperty.getZkService())
                .build();
    }

    /**
     * Create id worker.
     * 构建id生成器
     *
     * @param curatorFramework the curator framework
     * @return the snowflake id worker
     * @throws Exception the exception
     */
    @Bean
    @ConditionalOnMissingBean
    public SnowflakeIdWorker createIdWorker(CuratorFramework curatorFramework) throws Exception {

        long baseId = createBaseId(curatorFramework);
        if (baseId == -1) {
            throw new RuntimeException("create snowFlakeId fail, because baseId is illegal");
        }
        // TODO: 2018/1/5 应将dataCenterId割离以对应多个节点
        //将baseId拆成centerId和workId以供id生成器使用
        long workId = baseId & SnowflakeIdWorker.MAX_WORKER_ID;
        baseId = baseId >> SnowflakeIdWorker.WORKER_ID_BITS;
        long dataCenterId = baseId & SnowflakeIdWorker.MAX_DATA_CENTER_ID;

        SnowflakeIdWorker.init(workId, dataCenterId);
        return SnowflakeIdWorker.getInstance();
    }

    /**
     * Create base id.
     * 通过zk来分配base id
     *
     * @param curatorFramework the curator framework
     * @return the long
     * @throws Exception the exception
     */
    private long createBaseId(CuratorFramework curatorFramework) throws Exception {

        final String allWorkFolder = "/work/all";
        final String nowWorkFolder = "/work/now";
        //获取baseId的最大值
        final long maxId = 1 << (SnowflakeIdWorker.DATA_CENTER_ID_BITS + SnowflakeIdWorker.WORKER_ID_BITS);
        //检测是否有所需的节点，无则建立
        curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(allWorkFolder);
        curatorFramework.checkExists().creatingParentContainersIfNeeded().forPath(nowWorkFolder);
        //获取已经使用过的baseId集合
        List<String> allWork = curatorFramework.getChildren().forPath(allWorkFolder);
        //获取当前正在使用的baseId集合
        List<String> nowWork = curatorFramework.getChildren().forPath(nowWorkFolder);
        long baseId = -1L;
        long nowMaxId = -1L;

        if (allWork != null && allWork.size() > 0) {
            //将已经使用过的baseId从小到大排序
            allWork.sort((o1, o2) -> {
                if (o1.length() > o2.length()) {
                    return 1;
                } else if (o1.length() < o2.length()) {
                    return -1;
                }
                return o1.compareTo(o2);
            });
            //获取当前已经使用的最大的baseId
            nowMaxId = Long.valueOf(allWork.get(allWork.size() - 1));
            //从已使用过的baseId集合中剔除当前正在使用的baseId集合
            if (nowWork != null && nowWork.size() > 0) {
                allWork.removeIf(nowWork::contains);
            }
            //如果当前还有baseId未被使用，则利用临时节点抢占选取的baseId
            if (allWork.size() > 0) {
                for (String id : allWork) {
                    final String path = nowWorkFolder + "/" + id;
                    try {
                        curatorFramework.create().withMode(CreateMode.EPHEMERAL)
                                .forPath(path);
                        baseId = Long.valueOf(id);
                        break;
                    } catch (Exception e) {
                        log.error("zk lock path fail, path is {}, because {}", path, e);
                    }
                }
            }
        }
        //如果没有未使用的baseId或者抢占失败，则尝试申请新的baseId
        if (baseId == -1) {
            nowMaxId++;
            //当当前的最大baseId小于id生成器所支持的最大baseId时则尝试申请
            while (nowMaxId < maxId) {
                baseId = nowMaxId;
                final String path = nowWorkFolder + "/" + baseId;
                final String allPath = allWorkFolder + "/" + baseId;
                try {
                    //申请并抢占新的baseId
                    curatorFramework.create().withMode(CreateMode.PERSISTENT)
                            .forPath(allPath);
                    curatorFramework.create().withMode(CreateMode.EPHEMERAL)
                            .forPath(path);
                    break;
                } catch (Exception e) {
                    log.error("zk create path fail, path is {}, because {}", path, e);
                }
                nowMaxId++;
            }
        }

        if (baseId >= maxId || nowMaxId >= maxId) {
            log.error("create snowflakeId fail, baseId is {}, nowMaxId is {}, maxId is {}", baseId, nowMaxId, maxId);
            return -1;
        } else {
            log.warn("create snowflakeId success, baseId is {}, nowMaxId is {}, maxId is {}", baseId, nowMaxId, maxId);
            return baseId;
        }
    }
}
