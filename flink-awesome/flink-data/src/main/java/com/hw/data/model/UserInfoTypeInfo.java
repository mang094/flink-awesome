//package com.hw.data.model;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//
//import java.sql.ResultSet;
//import java.sql.Timestamp;
//
//public class UserInfoTypeInfo extends TypeInformation<UserInfo> {
//
//    @Override
//    public UserInfo createInstance(ResultSet resultSet) throws Exception {
//        UserInfo userInfo = new UserInfo();
//        userInfo.setId(resultSet.getLong("id"));
//        userInfo.setName(resultSet.getString("name"));
//        userInfo.setAge(resultSet.getInt("age"));
//        userInfo.setEmail(resultSet.getString("email"));
//        userInfo.setUpdateTime(resultSet.getTimestamp("update_time"));
//        return userInfo;
//    }
//
//    @Override
//    public TypeInformation<?>[] getFieldTypes() {
//        return new TypeInformation<?>[] {
//            Types.LONG,        // id
//            Types.STRING,      // name
//            Types.INT,         // age
//            Types.STRING,      // email
//            Types.SQL_TIMESTAMP // update_time
//        };
//    }
//
//    @Override
//    public String[] getFieldNames() {
//        return new String[] {"id", "name", "age", "email", "update_time"};
//    }
//}