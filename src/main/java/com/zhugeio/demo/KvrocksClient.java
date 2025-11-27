package com.zhugeio.demo;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * KVRocks客户端 - Lettuce真异步版本
 *
 * ✅ 优势:
 * 1. 真正的异步IO,基于Netty
 * 2. 无需线程池,不会阻塞
 * 3. 支持百万级并发
 */
public class KvrocksClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final boolean isCluster;

    // Lettuce客户端 (transient, 在每个TaskManager上重新初始化)
    private transient RedisClusterClient clusterClient;
    private transient RedisClient standaloneClient;
    private transient StatefulRedisClusterConnection<String, String> clusterConnection;
    private transient StatefulRedisConnection<String, String> standaloneConnection;

    public KvrocksClient(String host, int port, boolean isCluster) {
        this.host = host;
        this.port = port;
        this.isCluster = isCluster;
    }

    /**
     * 初始化Lettuce连接
     */
    public void init() {
        if (isCluster) {
            // 集群模式
            clusterClient = RedisClusterClient.create(
                    RedisURI.Builder
                            .redis(host, port)
                            .withTimeout(Duration.ofSeconds(60))
                            .build()
            );

            // ✅ 使用 ClusterClientOptions
            clusterClient.setOptions(ClusterClientOptions.builder()
                    .autoReconnect(true)
                    .pingBeforeActivateConnection(true)
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(60000)))
                    .build());

            clusterConnection = clusterClient.connect();

            System.out.println(String.format(
                    "✅ Lettuce集群连接初始化成功：%s:%d (真异步模式)", host, port
            ));

        } else {
            // 单机模式
            standaloneClient = RedisClient.create(
                    RedisURI.Builder
                            .redis(host, port)
                            .withTimeout(Duration.ofSeconds(5))
                            .build()
            );

            standaloneClient.setOptions(ClientOptions.builder()
                    .autoReconnect(true)
                    .pingBeforeActivateConnection(true)
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(60000)))
                    .build());

            standaloneConnection = standaloneClient.connect();

            System.out.println(String.format(
                    "✅ Lettuce单机连接初始化成功：%s:%d (真异步模式)", host, port
            ));
        }
    }

    /**
     * ✅ 真正的异步Get操作 (基于Netty,无阻塞)
     */
    public CompletableFuture<String> asyncGet(String key) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            System.err.println("KVRocks GET失败: " + key + ", " + ex.getMessage());
                            return null;
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            System.err.println("KVRocks GET失败: " + key + ", " + ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * ✅ 真正的异步Set操作 (修复类型转换)
     */
    public CompletableFuture<Void> asyncSet(String key, String value) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)  // ✅ 修复: 显式转换为Void
                        .exceptionally(ex -> {
                            System.err.println("KVRocks SET失败: " + key + ", " + ex.getMessage());
                            return null;
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)  // ✅ 修复
                        .exceptionally(ex -> {
                            System.err.println("KVRocks SET失败: " + key + ", " + ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * ✅ 异步Hash Get
     */
    public CompletableFuture<String> asyncHGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * ✅ 异步Hash Set (修复类型转换)
     */
    public CompletableFuture<Void> asyncHSet(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)  // ✅ 修复
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)  // ✅ 修复
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * ✅ 批量Hash Get (真正的Pipeline)
     */
    public CompletableFuture<Map<String, String>> asyncBatchHGet(String key, List<String> fields) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();

                // Pipeline多个HGET命令
                List<RedisFuture<String>> futures = fields.stream()
                        .map(field -> async.hget(key, field))
                        .collect(Collectors.toList());

                // 等待所有完成
                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        try {
                            String value = futures.get(i).get();
                            if (value != null) {
                                result.put(fields.get(i), value);
                            }
                        } catch (Exception ignored) {}
                    }
                    return result;
                });

            } else {
                // 单机模式使用真正的Pipeline
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);  // 开启Pipeline

                List<RedisFuture<String>> futures = fields.stream()
                        .map(field -> async.hget(key, field))
                        .collect(Collectors.toList());

                async.flushCommands();  // 一次性发送
                async.setAutoFlushCommands(true);

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        try {
                            String value = futures.get(i).get();
                            if (value != null) {
                                result.put(fields.get(i), value);
                            }
                        } catch (Exception ignored) {}
                    }
                    return result;
                });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new HashMap<>());
        }
    }

    /**
     * ✅ 批量Hash Set (修复类型转换)
     */
    public CompletableFuture<Void> asyncBatchHSet(String key, Map<String, String> fieldValues) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();

                List<RedisFuture<Boolean>> futures = fieldValues.entrySet().stream()
                        .map(entry -> async.hset(key, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> (Void) null);  // ✅ 修复

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = fieldValues.entrySet().stream()
                        .map(entry -> async.hset(key, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

                async.flushCommands();
                async.setAutoFlushCommands(true);

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> (Void) null);  // ✅ 修复
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 测试连接
     */
    public boolean testConnection() {
        try {
            if (isCluster) {
                String pong = clusterConnection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            } else {
                String pong = standaloneConnection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            }
        } catch (Exception e) {
            System.err.println("KVRocks连接测试失败: " + e.getMessage());
            return false;
        }
    }

    /**
     * ✅ 同步批量管道查询 (Window算子专用)
     *
     * 使用真正的Pipeline一次性发送多个命令，然后同步等待结果
     * 性能远高于逐个查询
     */
    public Map<String, String> syncBatchHGet(String hashKey, List<String> fields, long timeoutMs) {
        Map<String, String> results = new HashMap<>();

        if (fields == null || fields.isEmpty()) {
            return results;
        }

        try {
            if (isCluster) {
                // 集群模式: Pipeline
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                async.setAutoFlushCommands(false);  // 开启Pipeline

                // 批量发送命令
                List<RedisFuture<String>> futures = new ArrayList<>();
                for (String field : fields) {
                    futures.add(async.hget(hashKey, field));
                }

                async.flushCommands();  // 一次性发送所有命令
                async.setAutoFlushCommands(true);

                // 同步等待所有结果
                for (int i = 0; i < fields.size(); i++) {
                    try {
                        String value = futures.get(i).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (value != null) {
                            results.put(hashKey + ":" + fields.get(i), value);
                        }
                    } catch (Exception e) {
                        // 单个查询失败不影响其他
                    }
                }

            } else {
                // 单机模式: Pipeline
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<String>> futures = new ArrayList<>();
                for (String field : fields) {
                    futures.add(async.hget(hashKey, field));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                // 同步等待
                for (int i = 0; i < fields.size(); i++) {
                    try {
                        String value = futures.get(i).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (value != null) {
                            results.put(hashKey + ":" + fields.get(i), value);
                        }
                    } catch (Exception e) {
                        // 继续
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("批量查询失败: " + hashKey + ", " + e.getMessage());
        }

        return results;
    }

    /**
     * ✅ 同步批量管道写入 (Window算子专用)
     */
    public void syncBatchHSet(String hashKey, Map<String, String> fieldValues, long timeoutMs) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            return;
        }

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
                    futures.add(async.hset(hashKey, entry.getKey(), entry.getValue()));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                // 等待所有完成
                CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
                    futures.add(async.hset(hashKey, entry.getKey(), entry.getValue()));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

        } catch (Exception e) {
            System.err.println("批量写入失败: " + hashKey + ", " + e.getMessage());
        }
    }

    /**
     * 关闭连接
     */
    public void shutdown() {
        try {
            if (clusterConnection != null) {
                clusterConnection.close();
            }
            if (clusterClient != null) {
                clusterClient.shutdown();
                System.out.println("Lettuce集群连接已关闭");
            }

            if (standaloneConnection != null) {
                standaloneConnection.close();
            }
            if (standaloneClient != null) {
                standaloneClient.shutdown();
                System.out.println("Lettuce单机连接已关闭");
            }
        } catch (Exception e) {
            System.err.println("关闭Lettuce连接失败: " + e.getMessage());
        }
    }
}