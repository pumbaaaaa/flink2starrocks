package com.pumbaa.starrocks;

import lombok.Data;

@Data
public class TableConf {

    private String sourceTable;
    private String instanceIp;
    private String dbName;
    private String serverId;
    private String odsTablePrefix;
    private String sourceUid;
    private String filterUid;
    private String flatmapUid;
    private String sinkUid;
}
