package com.zhugeio.demo.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 原始事件（模拟真实ETL事件结构）
 * 包含150+个字段
 */
public class RawEvent {

    // ========== 基础字段 (20个) ==========
    private String appKey;
    private Integer appId;
    private String owner;
    private Integer sdk;           // 平台：1-Android, 2-iOS, 3-Web

    // 用户标识
    private String did;            // 设备ID
    private String cuid;           // 用户ID (可能为null)
    private Long sid;              // 会话ID (时间戳)

    // 事件信息
    private String eventName;      // 事件名称
    private String eventType;      // 事件类型: evt/mkt/abp
    private Long timestamp;        // 事件时间戳
    private Long ingestTime;       // 进入系统时间

    // IP和地理位置
    private String ip;             // IP地址
    private String country;        // 国家
    private String province;       // 省份
    private String city;           // 城市
    private String carrier;        // 运营商

    // 设备信息
    private String ua;             // UserAgent
    private String osVersion;      // 操作系统版本
    private String brand;          // 设备品牌
    private String model;          // 设备型号

    // ========== 用户属性 (30个) ==========
    private String gender;         // 性别
    private Integer age;           // 年龄
    private String birthday;       // 生日
    private String phone;          // 手机号
    private String email;          // 邮箱
    private String name;           // 姓名
    private String nickname;       // 昵称
    private String avatar;         // 头像
    private String address;        // 地址
    private String company;        // 公司
    private String position;       // 职位
    private String department;     // 部门
    private String education;      // 学历
    private String income;         // 收入
    private String marriage;       // 婚姻状况
    private String hobby;          // 爱好
    private String tag1;           // 标签1
    private String tag2;           // 标签2
    private String tag3;           // 标签3
    private String tag4;           // 标签4
    private String tag5;           // 标签5
    private String level;          // 用户等级
    private Integer vipStatus;     // VIP状态
    private Long registerTime;     // 注册时间
    private Long lastLoginTime;    // 最后登录时间
    private Integer loginCount;    // 登录次数
    private Double balance;        // 余额
    private Double points;         // 积分
    private String channel;        // 渠道
    private String source;         // 来源

    // ========== 设备属性 (20个) ==========
    private Integer deviceId;       // 设备唯一标识
    private String idfa;           // iOS广告标识
    private String androidId;      // Android ID
    private String oaid;           // OAID
    private String imei;           // IMEI
    private String mac;            // MAC地址
    private String screenWidth;    // 屏幕宽度
    private String screenHeight;   // 屏幕高度
    private String language;       // 语言
    private String timezone;       // 时区
    private String network;        // 网络类型
    private String wifi;           // WiFi名称
    private String isp;            // 运营商
    private String appVersion;     // 应用版本
    private String sdkVersion;     // SDK版本
    private String osName;         // 操作系统名称
    private Integer ram;           // 内存
    private Integer storage;       // 存储
    private Integer battery;       // 电量
    private String cpuModel;       // CPU型号

    // ========== 事件属性 (80个，模拟cus1-cus100) ==========
    private Map<String, Object> customProps;  // 自定义属性 cus1-cus100

    // ========== 构造函数 ==========
    public RawEvent() {
        this.customProps = new HashMap<>();
    }

    public RawEvent(String appKey, Integer appId, String did, String cuid,
                    Long sid, String eventName, long timestamp) {
        this();
        this.appKey = appKey;
        this.appId = appId;
        this.did = did;
        this.cuid = cuid;
        this.sid = sid;
        this.eventName = eventName;
        this.timestamp = timestamp;
        this.ingestTime = System.currentTimeMillis();
    }

    // ========== Getters and Setters ==========
    public String getAppKey() { return appKey; }
    public void setAppKey(String appKey) { this.appKey = appKey; }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public Integer getSdk() { return sdk; }
    public void setSdk(Integer sdk) { this.sdk = sdk; }

    public String getDid() { return did; }
    public void setDid(String did) { this.did = did; }

    public String getCuid() { return cuid; }
    public void setCuid(String cuid) { this.cuid = cuid; }

    public Long getSid() { return sid; }
    public void setSid(Long sid) { this.sid = sid; }

    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public long getIngestTime() { return ingestTime; }
    public void setIngestTime(long ingestTime) { this.ingestTime = ingestTime; }

    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public String getProvince() { return province; }
    public void setProvince(String province) { this.province = province; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getCarrier() { return carrier; }
    public void setCarrier(String carrier) { this.carrier = carrier; }

    public String getUa() { return ua; }
    public void setUa(String ua) { this.ua = ua; }

    public String getOsVersion() { return osVersion; }
    public void setOsVersion(String osVersion) { this.osVersion = osVersion; }

    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }

    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }

    public String getGender() { return gender; }
    public void setGender(String gender) { this.gender = gender; }

    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }

    public String getBirthday() { return birthday; }
    public void setBirthday(String birthday) { this.birthday = birthday; }

    public String getPhone() { return phone; }
    public void setPhone(String phone) { this.phone = phone; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getNickname() { return nickname; }
    public void setNickname(String nickname) { this.nickname = nickname; }

    public String getAvatar() { return avatar; }
    public void setAvatar(String avatar) { this.avatar = avatar; }

    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }

    public String getCompany() { return company; }
    public void setCompany(String company) { this.company = company; }

    public String getPosition() { return position; }
    public void setPosition(String position) { this.position = position; }

    public String getDepartment() { return department; }
    public void setDepartment(String department) { this.department = department; }

    public String getEducation() { return education; }
    public void setEducation(String education) { this.education = education; }

    public String getIncome() { return income; }
    public void setIncome(String income) { this.income = income; }

    public String getMarriage() { return marriage; }
    public void setMarriage(String marriage) { this.marriage = marriage; }

    public String getHobby() { return hobby; }
    public void setHobby(String hobby) { this.hobby = hobby; }

    public String getTag1() { return tag1; }
    public void setTag1(String tag1) { this.tag1 = tag1; }

    public String getTag2() { return tag2; }
    public void setTag2(String tag2) { this.tag2 = tag2; }

    public String getTag3() { return tag3; }
    public void setTag3(String tag3) { this.tag3 = tag3; }

    public String getTag4() { return tag4; }
    public void setTag4(String tag4) { this.tag4 = tag4; }

    public String getTag5() { return tag5; }
    public void setTag5(String tag5) { this.tag5 = tag5; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public Integer getVipStatus() { return vipStatus; }
    public void setVipStatus(Integer vipStatus) { this.vipStatus = vipStatus; }

    public Long getRegisterTime() { return registerTime; }
    public void setRegisterTime(Long registerTime) { this.registerTime = registerTime; }

    public Long getLastLoginTime() { return lastLoginTime; }
    public void setLastLoginTime(Long lastLoginTime) { this.lastLoginTime = lastLoginTime; }

    public Integer getLoginCount() { return loginCount; }
    public void setLoginCount(Integer loginCount) { this.loginCount = loginCount; }

    public Double getBalance() { return balance; }
    public void setBalance(Double balance) { this.balance = balance; }

    public Double getPoints() { return points; }
    public void setPoints(Double points) { this.points = points; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public Integer getDeviceId() { return deviceId; }
    public void setDeviceId(Integer deviceId) { this.deviceId = deviceId; }

    public String getIdfa() { return idfa; }
    public void setIdfa(String idfa) { this.idfa = idfa; }

    public String getAndroidId() { return androidId; }
    public void setAndroidId(String androidId) { this.androidId = androidId; }

    public String getOaid() { return oaid; }
    public void setOaid(String oaid) { this.oaid = oaid; }

    public String getImei() { return imei; }
    public void setImei(String imei) { this.imei = imei; }

    public String getMac() { return mac; }
    public void setMac(String mac) { this.mac = mac; }

    public String getScreenWidth() { return screenWidth; }
    public void setScreenWidth(String screenWidth) { this.screenWidth = screenWidth; }

    public String getScreenHeight() { return screenHeight; }
    public void setScreenHeight(String screenHeight) { this.screenHeight = screenHeight; }

    public String getLanguage() { return language; }
    public void setLanguage(String language) { this.language = language; }

    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }

    public String getNetwork() { return network; }
    public void setNetwork(String network) { this.network = network; }

    public String getWifi() { return wifi; }
    public void setWifi(String wifi) { this.wifi = wifi; }

    public String getIsp() { return isp; }
    public void setIsp(String isp) { this.isp = isp; }

    public String getAppVersion() { return appVersion; }
    public void setAppVersion(String appVersion) { this.appVersion = appVersion; }

    public String getSdkVersion() { return sdkVersion; }
    public void setSdkVersion(String sdkVersion) { this.sdkVersion = sdkVersion; }

    public String getOsName() { return osName; }
    public void setOsName(String osName) { this.osName = osName; }

    public Integer getRam() { return ram; }
    public void setRam(Integer ram) { this.ram = ram; }

    public Integer getStorage() { return storage; }
    public void setStorage(Integer storage) { this.storage = storage; }

    public Integer getBattery() { return battery; }
    public void setBattery(Integer battery) { this.battery = battery; }

    public String getCpuModel() { return cpuModel; }
    public void setCpuModel(String cpuModel) { this.cpuModel = cpuModel; }

    public Map<String, Object> getCustomProps() { return customProps; }
    public void setCustomProps(Map<String, Object> customProps) { this.customProps = customProps; }

    public void putCustomProp(String key, Object value) {
        this.customProps.put(key, value);
    }
}