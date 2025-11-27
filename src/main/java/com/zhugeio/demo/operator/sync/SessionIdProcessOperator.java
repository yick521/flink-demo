package com.zhugeio.demo.operator.sync;

import com.zhugeio.demo.model.IdOutput;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.NumberFormat;

/**
 * 会话ID处理算子（同步）
 */
public class SessionIdProcessOperator extends ProcessFunction<IdOutput, IdOutput> {

    @Override
    public void processElement(IdOutput value, Context ctx, Collector<IdOutput> out) {

        if (value.getSid() != null && value.getZgDeviceId() != null) {
            Long zgSessionId = generateSessionId(value.getSid(), value.getZgDeviceId());
            value.setZgSessionId(zgSessionId);
        } else {
            value.setZgSessionId(-1L);
        }

        out.collect(value);
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