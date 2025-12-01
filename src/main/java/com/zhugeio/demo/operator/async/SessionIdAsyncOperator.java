package com.zhugeio.demo.operator.async;

import com.zhugeio.demo.model.IdOutput;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * 会话ID生成算子
 *
 * 注意: 此算子仅做纯计算（基于sid和zgDeviceId生成sessionId）
 * 不涉及任何KVRocks写入操作，因此无需优雅关闭机制
 */
public class SessionIdAsyncOperator extends RichAsyncFunction<IdOutput, IdOutput> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionIdAsyncOperator.class);

    @Override
    public void asyncInvoke(IdOutput input, ResultFuture<IdOutput> resultFuture) {
        CompletableFuture.supplyAsync(() -> {
            if (input.getSid() != null && input.getZgDeviceId() != null) {
                Long zgSessionId = generateSessionId(input.getSid(), input.getZgDeviceId());
                input.setZgSessionId(zgSessionId);
            } else {
                input.setZgSessionId(-1L);
            }
            return input;
        }).whenComplete((output, throwable) -> {
            if (throwable != null) {
                resultFuture.completeExceptionally(throwable);
            } else {
                resultFuture.complete(Collections.singleton(output));
            }
        });
    }

    /**
     * 生成会话ID: (sid/1000) + zgDeviceId(9位)
     * 例如: sid=1638360000000, zgDeviceId=123456789
     *      → 1638360000 + "000123456789" → 1638360000000123456789
     */
    private Long generateSessionId(Long sid, Long zgDeviceId) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumIntegerDigits(9);
        nf.setMinimumIntegerDigits(9);
        String sessionIdStr = (sid / 1000) + nf.format(zgDeviceId);
        return Long.parseLong(sessionIdStr);
    }
}