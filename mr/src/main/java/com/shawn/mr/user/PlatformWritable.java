package com.shawn.mr.user;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PlatformWritable implements Writable {
    private String platformId;
    private String platformName;
    private String type;
    private String provinceId;

    public PlatformWritable(String platformId, String platformName, String type, String provinceId) {
        this.platformId = platformId;
        this.platformName = platformName;
        this.type = type;
        this.provinceId = provinceId;
    }

    public PlatformWritable() {

    }

    public String getPlatformId() {
        return platformId;
    }

    public void setPlatformId(String platformId) {
        this.platformId = platformId;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProvinceId() {
        return provinceId;
    }

    public void setProvinceId(String provinceId) {
        this.provinceId = provinceId;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.platformId);
        dataOutput.writeUTF(this.platformName);
        dataOutput.writeUTF(this.type);
        dataOutput.writeUTF(this.provinceId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.platformId =dataInput.readUTF();
        this.platformName =dataInput.readUTF();
        this.type =dataInput.readUTF();
        this.provinceId =dataInput.readUTF();
    }

}
