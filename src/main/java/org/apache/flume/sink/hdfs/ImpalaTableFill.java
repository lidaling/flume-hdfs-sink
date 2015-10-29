/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.hdfs;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class ImpalaTableFill {
    private String nowPartition;
    private String lastPartition;
    private String impalaUrl;
    private String tableName_parquet;
    private String tableName_text;
    private String tableTxtLocation;
    public Boolean workable=true;
    private String partitionFormat;
    private static String columns;
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaTableFill.class);

    public ImpalaTableFill(String tableName, String tableTxtLocation, String impalaUrl,String partitionFormat) {
        this.tableTxtLocation = tableTxtLocation;
        this.impalaUrl = impalaUrl;
        this.partitionFormat=partitionFormat;
        if("".equals(tableName)||"".equals(tableTxtLocation)||"".equals(impalaUrl)){
            workable=false;
        }else {
            this.tableName_parquet=tableName.split(",")[0];
            this.tableName_text=tableName.split(",")[1];
            LOG.debug("check impala workable :yes");
            LOG.debug("tablenames check:"+this.tableName_parquet);
            LOG.debug("tablenames check:"+this.tableName_text);
        }
    }

    public void impalaTableFillData(String hdfsPath) {
        // make the columns str
        if(StringUtils.isBlank(columns)){
            columns=this.getColumnStr(this.tableName_text);
        }
        /**
         * check partition exists ,if not create one and load data
         * else load data only
         *
        */
        if(!this.checkNowPartitionExsits(this.tableName_text)) {
            this.createPartition(this.tableName_text);
            this.execTxtTableDataLoad(hdfsPath);
            if(!this.checkNowPartitionExsits(this.tableName_parquet)) {
                this.createPartition(this.tableName_parquet);
                TimeStage timeStage=getTimeStage(this.nowPartition);
                this.execParquetTableDataFill(timeStage.start,timeStage.end);
            }
        }else{
            this.execTxtTableDataLoad(hdfsPath);
        }
    }

    private TimeStage getTimeStage(String nowTimeStr) {
        TimeStage timestage=new TimeStage();
        try {
            Date date=new SimpleDateFormat(this.partitionFormat).parse(nowTimeStr);
            Calendar calendar=Calendar.getInstance();
            calendar.setTime(date);
            timestage.end=calendar.getTimeInMillis();
            Calendar calendarStart= (Calendar) calendar.clone();
            setLastTime(calendarStart);
            timestage.start=calendarStart.getTimeInMillis();
        } catch (ParseException e) {
            LOG.error(e.getMessage());
        }
        LOG.debug("time range:" + timestage.toString());
        return timestage;
    }

    public void execParquetTableDataFill(long start,long end){
        Connection con = this.getConnection();
        Statement stmt = null;
        String sql = "insert overwrite "+this.tableName_parquet+" partition (dat= \'"+this.lastPartition+"\') select "+columns+" from "+this.tableName_text+
                " where (dat =\'"+this.nowPartition+"\' or dat = \'" +this.lastPartition+
                "\') and createTime >=" +start +" and createTime <"+end;
        LOG.debug("exec sql :"+sql);
        try {
            stmt = con.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException var14) {
            LOG.error(var14.getMessage());
        } finally {
            try {
                this.closeResource(con, stmt, (ResultSet)null);
            } catch (SQLException var13) {
                LOG.error(var13.getMessage());
            }

        }

    }

    private void execTxtTableDataLoad(String hdfsPath) {
        Connection con = this.getConnection();
        Statement stmt = null;
        String sql = "load data inpath '" + hdfsPath + "' into table " + this.tableName_text + " partition(dat = \'" + this.nowPartition + "\');";
        LOG.debug("exec sql :"+sql);
        try {
            stmt = con.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException var14) {
            LOG.error(var14.getMessage());
        } finally {
            try {
                this.closeResource(con, stmt, (ResultSet)null);
            } catch (SQLException var13) {
                LOG.error(var13.getMessage());
            }

        }

    }

    private Connection getConnection() {
        Connection con = null;

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            con = DriverManager.getConnection(this.impalaUrl);
            LOG.debug("impalaurl:" + this.impalaUrl);
        } catch (ClassNotFoundException var3) {
            LOG.error(var3.toString());
        } catch (SQLException var4) {
            LOG.error(var4.toString());
        }

        return con;
    }

    private void closeResource(Connection con, Statement stmt, ResultSet rs) throws SQLException {
        if(rs != null) {
            rs.close();
        }

        if(stmt != null) {
            stmt.close();
        }

        if(con != null) {
            con.close();
        }

    }

    private void createPartition(String tableName) {
        Connection con = this.getConnection();
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate("alter table " + tableName + " add partition(dat =\'" + this.nowPartition + "\')");
        } catch (SQLException var13) {
            LOG.error(var13.getMessage());
        } finally {
            try {
                this.closeResource(con, stmt, (ResultSet)null);
            } catch (SQLException var12) {
                LOG.error(var12.getMessage());
            }
        }
    }

    private boolean checkNowPartitionExsits(String tableName) {
        Connection con = this.getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        this.setPartition();
        boolean result = false;
        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery("show partitions " + tableName);

            while(rs.next()) {
                LOG.debug("rs.getString(1):"+rs.getString(1));
                if(rs.getString(1) .equals( this.nowPartition)) {
                    result = true;
                    break;
                }
            }
        } catch (SQLException var14) {
            LOG.error(var14.getMessage());
        } finally {
            try {
                this.closeResource(con, stmt, rs);
            } catch (SQLException var13) {
                LOG.error(var13.getMessage());
            }

        }
        return result;
    }

    private void setPartition() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(this.partitionFormat);
        this.nowPartition = simpleDateFormat.format(calendar.getTime());
        Calendar calendarLast= (Calendar) calendar.clone();
        setLastTime(calendarLast);
        this.lastPartition=simpleDateFormat.format(calendarLast.getTime());
    }
    private String getColumnStr(String tableName){
        Connection con = this.getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        this.setPartition();
        boolean result = false;
        StringBuffer sbf=null;
        String columnStr="";
        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery("SHOW COLUMN STATS " + tableName);
            sbf=new StringBuffer();
            while(rs.next()) {
                LOG.debug("rs.getString(1):"+rs.getString(1));
                if(!"dat".equals(rs.getString(1))){
                    sbf.append(",");
                    sbf.append(rs.getString(1));
                }
            }
        } catch (SQLException var14) {
            LOG.error(var14.getMessage());
        } finally {
            try {
                this.closeResource(con, stmt, rs);
            } catch (SQLException var13) {
                LOG.error(var13.getMessage());
            }

        }
        return sbf.toString().substring(1);
    }
    private void setLastTime(Calendar calendar){
        /**
         * date 5
         * hour 10
         * minute 12
         */
        if("yyyyMMddHHmm".equals(this.partitionFormat)){
            calendar.add(Calendar.MINUTE, -1);
        }else if("yyyyMMddHH".equals(this.partitionFormat)){
            calendar.add(Calendar.HOUR, -1);
        }else if("yyyyMMdd".equals(this.partitionFormat)){
            calendar.add(Calendar.DATE, -1);
        }else if("yyyyMM".equals(this.partitionFormat)){
            calendar.add(Calendar.MONTH, -1);
        }
    }
    class TimeStage{
        public long start;
        public long end;

        @Override
        public String toString() {
            return "TimeStage{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}
