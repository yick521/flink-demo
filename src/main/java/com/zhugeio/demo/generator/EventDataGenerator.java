package com.zhugeio.demo.generator;

import com.zhugeio.demo.model.RawEvent;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 事件数据生成器
 * 生成包含150+个字段的真实ETL事件
 *
 * 性能目标：
 * - 每并行度QPS: 3000
 * - 总并行度: 16
 * - 总QPS: 48000
 * - 运行时间: 3小时
 * - 总数据量: 48000 * 3600 * 3 = 5.184亿
 */
public class EventDataGenerator extends RichParallelSourceFunction<RawEvent> {

    private volatile boolean running = true;
    private final int qps;
    private final long maxRecords;  // 每个并行度最大生成记录数

    // 模拟数据池
    private static final String[] EVENTS = {
            "page_view", "button_click", "form_submit", "video_play", "video_pause",
            "add_to_cart", "purchase", "search", "download", "share",
            "register", "login", "logout", "like", "comment"
    };

    private static final String[] BRANDS = {
            "Apple", "Samsung", "Huawei", "Xiaomi", "OPPO", "VIVO", "OnePlus", "Realme"
    };

    private static final String[] MODELS = {
            "iPhone 14", "Galaxy S23", "Mate 50", "Mi 13", "Find X6", "X90", "11 Pro", "GT Neo5"
    };

    private static final String[] CITIES = {
            "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Chengdu", "Wuhan", "Xi'an"
    };

    private static final String[] CHANNELS = {
            "WeChat", "Weibo", "Douyin", "Baidu", "Google", "Facebook", "AppStore", "Direct"
    };

    public EventDataGenerator(int qps) {
        this(qps, Long.MAX_VALUE);
    }

    public EventDataGenerator(int qps, long maxRecords) {
        this.qps = qps;
        this.maxRecords = maxRecords;
    }

    @Override
    public void run(SourceContext<RawEvent> ctx) throws Exception {

        Random random = new Random();
        long interval = 1000 / qps;

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        long recordCount = 0;
        long startTime = System.currentTimeMillis();

        System.out.println(String.format(
                "[Generator-%d] 启动，目标QPS: %d, 最大记录数: %d",
                subtaskIndex, qps, maxRecords
        ));

        while (running && recordCount < maxRecords) {
            long iterStart = System.currentTimeMillis();

            // 生成事件
            RawEvent event = generateRichEvent(random, subtaskIndex);
            ctx.collect(event);

            recordCount++;

            // 每10万条打印一次进度
            if (recordCount % 100000 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                double actualQps = recordCount * 1000.0 / elapsed;
                System.out.println(String.format(
                        "[Generator-%d] 已生成: %d 条, 实际QPS: %.2f, 运行时间: %d 秒",
                        subtaskIndex, recordCount, actualQps, elapsed / 1000
                ));
            }

            // 控制速率
            long elapsed = System.currentTimeMillis() - iterStart;
            long sleep = interval - elapsed;
            if (sleep > 0) {
                TimeUnit.MILLISECONDS.sleep(sleep);
            }
        }

        long totalElapsed = System.currentTimeMillis() - startTime;
        double avgQps = recordCount * 1000.0 / totalElapsed;
        System.out.println(String.format(
                "[Generator-%d] 完成！总记录数: %d, 平均QPS: %.2f, 总耗时: %d 秒",
                subtaskIndex, recordCount, avgQps, totalElapsed / 1000
        ));
    }

    /**
     * 生成包含150+个字段的真实事件
     */
    private RawEvent generateRichEvent(Random random, int subtaskIndex) {

        RawEvent event = new RawEvent();

        // ========== 基础字段 ==========
        event.setAppKey("test_app_key");
        event.setAppId(1);
        event.setOwner("zg");
        event.setSdk(random.nextInt(3) + 1);  // 1-Android, 2-iOS, 3-Web

        // 80%使用已有设备（模拟缓存命中）
        if (random.nextDouble() < 0.8) {
            event.setDid("device_" + random.nextInt(100000));
        } else {
            event.setDid("device_new_" + System.nanoTime() + "_" + subtaskIndex);
        }

        // 50%有用户ID
        if (random.nextDouble() < 0.5) {
            if (random.nextDouble() < 0.5) {
                event.setCuid("user_" + random.nextInt(50000));
            } else {
                event.setCuid("user_new_" + System.nanoTime() + "_" + subtaskIndex);
            }
        }

        event.setSid(System.currentTimeMillis());
        event.setEventName(EVENTS[random.nextInt(EVENTS.length)]);
        event.setEventType("evt");
        event.setTimestamp(System.currentTimeMillis());
        event.setIngestTime(System.currentTimeMillis());

        // ========== IP和地理位置 ==========
        event.setIp(generateRandomIp(random));
        event.setCountry("China");
        event.setProvince("Guangdong");
        event.setCity(CITIES[random.nextInt(CITIES.length)]);
        event.setCarrier("China Mobile");

        // ========== 设备信息 ==========
        event.setUa(generateUserAgent(random));
        event.setOsVersion(generateOsVersion(random, event.getSdk()));
        event.setBrand(BRANDS[random.nextInt(BRANDS.length)]);
        event.setModel(MODELS[random.nextInt(MODELS.length)]);

        // ========== 用户属性 (30个) ==========
        event.setGender(random.nextBoolean() ? "male" : "female");
        event.setAge(18 + random.nextInt(50));
        event.setBirthday(String.format("19%02d-%02d-%02d",
                70 + random.nextInt(30), 1 + random.nextInt(12), 1 + random.nextInt(28)));
        event.setPhone(String.format("138%08d", random.nextInt(100000000)));
        event.setEmail(String.format("user%d@example.com", random.nextInt(100000)));
        event.setName("User_" + random.nextInt(100000));
        event.setNickname("Nickname_" + random.nextInt(100000));
        event.setAvatar("https://cdn.example.com/avatar/" + random.nextInt(1000) + ".jpg");
        event.setAddress(CITIES[random.nextInt(CITIES.length)] + " Street " + random.nextInt(1000));
        event.setCompany("Company_" + random.nextInt(1000));
        event.setPosition("Position_" + random.nextInt(100));
        event.setDepartment("Dept_" + random.nextInt(50));
        event.setEducation(new String[]{"Bachelor", "Master", "PhD"}[random.nextInt(3)]);
        event.setIncome(String.format("%.0f", 5000 + random.nextDouble() * 50000));
        event.setMarriage(random.nextBoolean() ? "married" : "single");
        event.setHobby("Hobby_" + random.nextInt(100));
        event.setTag1("Tag1_" + random.nextInt(50));
        event.setTag2("Tag2_" + random.nextInt(50));
        event.setTag3("Tag3_" + random.nextInt(50));
        event.setTag4("Tag4_" + random.nextInt(50));
        event.setTag5("Tag5_" + random.nextInt(50));
        event.setLevel("Level_" + random.nextInt(10));
        event.setVipStatus(random.nextInt(3));
        event.setRegisterTime(System.currentTimeMillis() - random.nextInt(365 * 24 * 3600) * 1000L);
        event.setLastLoginTime(System.currentTimeMillis() - random.nextInt(7 * 24 * 3600) * 1000L);
        event.setLoginCount(random.nextInt(1000));
        event.setBalance(random.nextDouble() * 10000);
        event.setPoints(random.nextDouble() * 100000);
        event.setChannel(CHANNELS[random.nextInt(CHANNELS.length)]);
        event.setSource("Source_" + random.nextInt(20));

        // ========== 设备属性 (20个) ==========
        event.setDeviceId(random.nextInt(10000));
        event.setIdfa(generateUUID(random));
        event.setAndroidId(generateUUID(random));
        event.setOaid(generateUUID(random));
        event.setImei(String.format("%015d", random.nextLong() & Long.MAX_VALUE).substring(0, 15));
        event.setMac(generateMacAddress(random));
        event.setScreenWidth(new String[]{"1080", "1440", "2160"}[random.nextInt(3)]);
        event.setScreenHeight(new String[]{"1920", "2560", "3840"}[random.nextInt(3)]);
        event.setLanguage(new String[]{"zh-CN", "en-US", "ja-JP"}[random.nextInt(3)]);
        event.setTimezone("Asia/Shanghai");
        event.setNetwork(new String[]{"WiFi", "4G", "5G"}[random.nextInt(3)]);
        event.setWifi("WiFi_" + random.nextInt(1000));
        event.setIsp(new String[]{"China Mobile", "China Unicom", "China Telecom"}[random.nextInt(3)]);
        event.setAppVersion("v" + (1 + random.nextInt(10)) + "." + random.nextInt(100));
        event.setSdkVersion("v" + (1 + random.nextInt(5)) + "." + random.nextInt(50));
        event.setOsName(event.getSdk() == 1 ? "Android" : event.getSdk() == 2 ? "iOS" : "Web");
        event.setRam(4 + random.nextInt(12));  // 4-16GB
        event.setStorage(64 + random.nextInt(5) * 64);  // 64-320GB
        event.setBattery(20 + random.nextInt(80));  // 20-100%
        event.setCpuModel("CPU_" + random.nextInt(50));

        // ========== 自定义属性 cus1-cus100 (80个) ==========
        for (int i = 1; i <= 80; i++) {
            String key = "cus" + i;
            Object value;

            // 随机生成不同类型的值
            int type = random.nextInt(4);
            switch (type) {
                case 0:  // String
                    value = "Value_" + random.nextInt(1000);
                    break;
                case 1:  // Integer
                    value = random.nextInt(10000);
                    break;
                case 2:  // Double
                    value = random.nextDouble() * 1000;
                    break;
                case 3:  // Boolean
                    value = random.nextBoolean();
                    break;
                default:
                    value = "Default_" + random.nextInt(100);
            }

            event.putCustomProp(key, value);
        }

        return event;
    }

    private String generateRandomIp(Random random) {
        return String.format("%d.%d.%d.%d",
                1 + random.nextInt(254),
                random.nextInt(256),
                random.nextInt(256),
                1 + random.nextInt(254));
    }

    private String generateUserAgent(Random random) {
        String[] uas = {
                "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36"
        };
        return uas[random.nextInt(uas.length)];
    }

    private String generateOsVersion(Random random, Integer sdk) {
        if (sdk == 1) {  // Android
            return "Android " + (9 + random.nextInt(5));
        } else if (sdk == 2) {  // iOS
            return "iOS " + (13 + random.nextInt(4));
        } else {  // Web
            return "Web";
        }
    }

    private String generateUUID(Random random) {
        return String.format("%08x-%04x-%04x-%04x-%012x",
                random.nextInt(),
                random.nextInt(0x10000),
                random.nextInt(0x10000),
                random.nextInt(0x10000),
                random.nextLong() & 0xFFFFFFFFFFFFL);
    }

    private String generateMacAddress(Random random) {
        return String.format("%02X:%02X:%02X:%02X:%02X:%02X",
                random.nextInt(256), random.nextInt(256), random.nextInt(256),
                random.nextInt(256), random.nextInt(256), random.nextInt(256));
    }

    @Override
    public void cancel() {
        running = false;
    }
}