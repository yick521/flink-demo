package com.zhugeio.demo.model;

public class IdOutput {
    private String appKey;
    private Integer appId;
    private String did;
    private String cuid;
    private Long sid;

    // 诸葛ID
    private Long zgDeviceId;
    private Long zgUserId;
    private Long zgSessionId;
    private Long zgid;

    // 标记
    private boolean isNewDevice;
    private boolean isNewUser;
    private boolean isNewZgid;

    private String eventName;
    private long timestamp;
    private long ingestTime;
    private long processTime;
    private long latency;

    // Getters and Setters
    public String getAppKey() { return appKey; }
    public void setAppKey(String appKey) { this.appKey = appKey; }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public String getDid() { return did; }
    public void setDid(String did) { this.did = did; }

    public String getCuid() { return cuid; }
    public void setCuid(String cuid) { this.cuid = cuid; }

    public Long getSid() { return sid; }
    public void setSid(Long sid) { this.sid = sid; }

    public Long getZgDeviceId() { return zgDeviceId; }
    public void setZgDeviceId(Long zgDeviceId) { this.zgDeviceId = zgDeviceId; }

    public Long getZgUserId() { return zgUserId; }
    public void setZgUserId(Long zgUserId) { this.zgUserId = zgUserId; }

    public Long getZgSessionId() { return zgSessionId; }
    public void setZgSessionId(Long zgSessionId) { this.zgSessionId = zgSessionId; }

    public Long getZgid() { return zgid; }
    public void setZgid(Long zgid) { this.zgid = zgid; }

    public boolean isNewDevice() { return isNewDevice; }
    public void setNewDevice(boolean newDevice) { isNewDevice = newDevice; }

    public boolean isNewUser() { return isNewUser; }
    public void setNewUser(boolean newUser) { isNewUser = newUser; }

    public boolean isNewZgid() { return isNewZgid; }
    public void setNewZgid(boolean newZgid) { isNewZgid = newZgid; }

    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public long getIngestTime() { return ingestTime; }
    public void setIngestTime(long ingestTime) { this.ingestTime = ingestTime; }

    public long getProcessTime() { return processTime; }
    public void setProcessTime(long processTime) { this.processTime = processTime; }

    public long getLatency() { return latency; }
    public void setLatency(long latency) { this.latency = latency; }
}