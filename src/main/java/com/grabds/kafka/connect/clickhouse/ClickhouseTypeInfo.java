package com.grabds.kafka.connect.clickhouse;

class ClickhouseTypeInfo
{
    public Integer idx;
    public String fieldName;
    public String strType;
    public Boolean isNullable;

    public ClickhouseTypeInfo(Integer idx, String fieldName, String strType, Boolean isNullable) {
        this.idx = idx;
        this.fieldName = fieldName;
        this.strType = strType;
        this.isNullable = isNullable;
    }
}
