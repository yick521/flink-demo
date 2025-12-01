package com.zhugeio.demo.operator.async;

import com.zhugeio.demo.model.IdOutput;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class SessionIdAsyncOperator extends RichAsyncFunction<IdOutput, IdOutput> {

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

    private Long generateSessionId(Long sid, Long zgDeviceId) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumIntegerDigits(9);
        nf.setMinimumIntegerDigits(9);
        String sessionIdStr = (sid / 1000) + nf.format(zgDeviceId);
        return Long.parseLong(sessionIdStr);
    }
}