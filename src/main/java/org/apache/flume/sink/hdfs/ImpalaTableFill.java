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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class ImpalaTableFill {
    private String nowPartition;
    private String tableLocation;
    private String tableName;
    private String impalaUrl;
    public Boolean workable=true;
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaTableFill.class);

    public ImpalaTableFill(String tableName, String tableLocation, String impalaUrl) {
        this.tableLocation = tableLocation;
        this.tableName = tableName;
        this.impalaUrl = impalaUrl;
        if("".equals(tableName)||"".equals(tableLocation)||"".equals(impalaUrl)){
            workable=false;
        }
    }

    public void impalaTableFillData(String hdfsPath) {
        if(!this.checkNowPartitionExsits()) {
            this.createPartition();
        }

        this.execDataLoad(hdfsPath);
    }

    private void execDataLoad(String hdfsPath) {
        Connection con = this.getConnection();
        Statement stmt = null;
        String sql = "load data inpath '" + hdfsPath + "' into table " + this.tableName + " partition(dat = \'" + this.nowPartition + "\');";
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

    private void createPartition() {
        Connection con = this.getConnection();
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            stmt.executeUpdate("alter table " + this.tableName + " add partition(dat =\'" + this.nowPartition + "\')");
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

    private boolean checkNowPartitionExsits() {
        Connection con = this.getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        this.setNowPartition();
        boolean result = false;

        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery("show partitions " + this.tableName);

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

    private void setNowPartition() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        this.nowPartition = simpleDateFormat.format(calendar.getTime());
    }
}
