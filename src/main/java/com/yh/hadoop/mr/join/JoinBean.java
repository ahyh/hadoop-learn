package com.yh.hadoop.mr.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBean implements Writable {

    private String orderId;

    private String userId;

    private String username;

    private Integer age;

    private String userFriend;

    private String tableName;

    public JoinBean() {
    }

    public void set(String orderId, String userId, String username, Integer age, String userFriend, String tableName) {
        this.orderId = orderId;
        this.userId = userId;
        this.username = username;
        this.age = age;
        this.userFriend = userFriend;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getUserFriend() {
        return userFriend;
    }

    public void setUserFriend(String userFriend) {
        this.userFriend = userFriend;
    }

    @Override
    public String toString() {
        return "JoinBean{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", username='" + username + '\'' +
                ", age=" + age +
                ", userFriend='" + userFriend + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.username);
        dataOutput.writeInt(this.age);
        dataOutput.writeUTF(this.userFriend);
        dataOutput.writeUTF(this.tableName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.username = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.userFriend = dataInput.readUTF();
        this.tableName = dataInput.readUTF();
    }
}
