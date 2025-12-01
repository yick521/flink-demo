package com.zhugeio.demo.sink;

import com.zhugeio.demo.model.IdOutput;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceMetricsSink extends RichSinkFunction<IdOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(PerformanceMetricsSink.class);

    private final String jobName;

    private transient AtomicLong recordCount;
    private transient AtomicLong totalLatency;
    private transient volatile long minLatency = Long.MAX_VALUE;
    private transient volatile long maxLatency = 0;
    private transient long startTime;
    private transient ScheduledExecutorService executor;

    public PerformanceMetricsSink(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        recordCount = new AtomicLong(0);
        totalLatency = new AtomicLong(0);
        startTime = System.currentTimeMillis();

        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this::printStats, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public void invoke(IdOutput value, Context context) throws Exception {
        long latency = value.getLatency();
        recordCount.incrementAndGet();
        totalLatency.addAndGet(latency);

        // 更新最小和最大延迟值
        if (latency < minLatency) {
            minLatency = latency;
        }
        if (latency > maxLatency) {
            maxLatency = latency;
        }
    }

    private void printStats() {
        long count = recordCount.get();
        if (count == 0) {
            return;
        }

        long elapsed = System.currentTimeMillis() - startTime;
        double qps = count * 1000.0 / elapsed;
        double avgLatency = (double) totalLatency.get() / count;

        // ✅ 修复：使用 String.format 格式化浮点数
        LOG.info("\n========== [{}] 性能统计 ==========\n" +
                        "总记录数: {}\n" +
                        "运行时间: {} 秒\n" +
                        "吞吐量: {} QPS\n" +
                        "延迟统计(ms):\n" +
                        "  - 最小: {}\n" +
                        "  - 最大: {}\n" +
                        "  - 平均: {}\n" +
                        "=====================================",
                jobName,
                count,
                elapsed / 1000,
                String.format("%.2f", qps),
                minLatency == Long.MAX_VALUE ? 0 : minLatency,
                maxLatency,
                String.format("%.2f", avgLatency));
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            printStats();
            executor.shutdown();
        }
    }
}