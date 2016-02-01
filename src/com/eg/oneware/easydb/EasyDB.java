package com.eg.oneware.easydb;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Mohamed.Abdelalim
 */
import com.eg.oneware.easydb.annotations.Table;
import com.eg.oneware.easydb.connections.EasyConnection;
import java.lang.annotation.Annotation;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.lang.reflect.Field;
import java.util.Arrays;

public final class EasyDB {

    private Connection connection = null;
    String serverName = "az01-scan";
    String portNumber = "1530";
    String serviceName = "opusl";
    String username = "customer";
    String password = "cust";

    public EasyDB(EasyConnection easyConnection) {
        try {

            String driverName = "oracle.jdbc.driver.OracleDriver";

            Class.forName(driverName);

            // Create a connection to the database
            String url = "jdbc:oracle:thin:@" + easyConnection.getServerName() + ":" + easyConnection.getPortNumber() + "/"
                    + easyConnection.getServiceName();

            setConnection(DriverManager.getConnection(url, easyConnection.getUsername(), easyConnection.getPassword()));
            connection.setAutoCommit(false);
        } catch (SQLException | ClassNotFoundException d) {
        }
    }

    public ArrayList getData(String SQLStatement) {
        ArrayList arry = new ArrayList();
        try {
            try (Statement statement = getConnection().createStatement()) {
                ResultSet result = statement.executeQuery(SQLStatement);
                ResultSetMetaData resultSetMetaData = result.getMetaData();

                HashMap dataMapStructure = new HashMap();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    dataMapStructure.put(resultSetMetaData.getColumnName(i),
                            resultSetMetaData.getColumnType(i));

                }
                while (result.next()) {
                    HashMap dataMap = new HashMap();
                    Set set = dataMapStructure.entrySet();
                    Iterator iterator = set.iterator();

                    while (iterator.hasNext()) {
                        Map.Entry map = (Map.Entry) iterator.next();

                        try {
                            dataMap.put(map.getKey(),
                                    result.getString((String) map.getKey()));
                        } catch (Exception e) {
                            dataMap.put(map.getKey(),
                                    result.getObject((String) map.getKey()));
                        }
                    }
                    arry.add(dataMap);
                }
            }
        } catch (SQLException e) {
        }
        return arry;

    }

    public ArrayList getData(Object obj, String SQLStatement) {
        ArrayList arry = new ArrayList();
        try {
            try (Statement statement = getConnection().createStatement()) {
                ResultSet result = statement.executeQuery(SQLStatement);
                ResultSetMetaData resultSetMetaData = result.getMetaData();

                Object objInstance = new Object();
                while (result.next()) {
                    try {
                        objInstance = obj.getClass().newInstance();
                    } catch (InstantiationException | IllegalAccessException e) {
                    }
                    Field[] flds = objInstance.getClass().getDeclaredFields();

                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        try {
                            char vchar = (char) 32;
                            String currentColumn = resultSetMetaData
                                    .getColumnName(i).toLowerCase().trim();

                            for (Field flod : flds) {
                                flod.setAccessible(true);
                                if (flod.getName()
                                        .replace("_", "")
                                        .equalsIgnoreCase(
                                                currentColumn.replace("_", ""))) {
                                    currentColumn = flod.getName();
                                    break;
                                }
                            }
                            Field currentField = objInstance.getClass().getDeclaredField(currentColumn);
                            currentField.setAccessible(true);
                            if (resultSetMetaData.getColumnType(i) == Types.VARCHAR) {

                                currentField.set(objInstance,
                                        result.getString(resultSetMetaData
                                                .getColumnName(i)));
                            } else if (resultSetMetaData.getColumnType(i) == Types.INTEGER) {
                                currentField
                                        .set(objInstance,
                                                result.getInt(resultSetMetaData
                                                        .getColumnName(i)));

                            } else if (resultSetMetaData.getColumnType(i) == Types.DATE || resultSetMetaData.getColumnType(i) == Types.TIMESTAMP) {
                                currentField
                                        .set(objInstance,
                                                result.getDate(resultSetMetaData
                                                        .getColumnName(i)));

                            } else if (resultSetMetaData.getColumnType(i) == Types.BOOLEAN) {
                                currentField
                                        .set(objInstance,
                                                result.getBoolean(resultSetMetaData
                                                        .getColumnName(i)));

                            } else if (resultSetMetaData.getColumnType(i) == Types.NUMERIC) {
                                currentField
                                        .set(objInstance,
                                                result.getBigDecimal(resultSetMetaData
                                                        .getColumnName(i)));

                            } else {
                                currentField
                                        .set(objInstance,
                                                (String) result
                                                .getObject(resultSetMetaData
                                                        .getColumnName(i)));
                            }

                        } catch (IllegalArgumentException | SecurityException | IllegalAccessException | NoSuchFieldException | SQLException e) {
                        }
                    }
                    arry.add(objInstance);
                }
            }
        } catch (SQLException e) {
        }
        return arry;
    }

    public boolean insert(Object obj) {

        String sqlInsert;
        String sqlValues;
        try {

            Object objInstance = new Object();
            try {
                objInstance = obj.getClass().newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
            }

            String tableName = this.getTableName(obj);
            sqlInsert = " insert into " + tableName + " (";
            sqlValues = " values (";
            ResultSet columnRs;
            columnRs = getConnection().getMetaData().getColumns(null, null, tableName, null);
            Field[] flds = objInstance.getClass().getDeclaredFields();

            while (columnRs.next()) {
                int autoIncrement = 0;
                String currentColumn = columnRs.getString("COLUMN_NAME").toLowerCase().trim();

                for (Field fil : flds) {
                    fil.setAccessible(true);
                    if (fil.getName().replace("_", "")
                            .equalsIgnoreCase(currentColumn.replace("_", ""))) {
                        Annotation[] ann = fil.getAnnotations();
                        if (ann.length > 0
                                && ann[0].toString()
                                .endsWith("AutoIncrement()")) {
                            sqlInsert = sqlInsert + "" + currentColumn + ",";
                            sqlValues = sqlValues + "" + Integer.toString(autoIncrement(currentColumn, tableName)) + ",";
                            autoIncrement = 1;
                        }
                        if (autoIncrement == 0) {
                            sqlInsert = sqlInsert + "" + currentColumn + ",";
                            try {
                                System.out.println("fields= " + fil);
                                if (fil.get(obj) != null) {
                                    if (fil.getType() == Date.class) {

                                        Date value = (Date) fil.get(obj);
                                        sqlValues = sqlValues + "to_date('"
                                                + (value.getYear() + 1900)
                                                + "-" + (value.getMonth() + 1)
                                                + "-" + value.getDate() + "','YYYY-MM-DD'),";
                                    } else if (fil.getType() == String.class) {
                                        String value = (String) fil
                                                .get(obj);
                                        sqlValues = sqlValues + "'" + value
                                                + "',";
                                    } else {

                                        Object value = fil.get(obj);
                                        sqlValues = sqlValues + "'" + value
                                                + "',";

                                    }

                                } else {

                                    sqlValues = sqlValues + "NULL,";
                                }
                            } catch (IllegalArgumentException e) {
                            } catch (IllegalAccessException e) {
                            }
                            break;
                        }
                    }
                }

            }

            String SqlReady = sqlInsert.substring(0, sqlInsert.length() - 1)
                    + ")" + sqlValues.substring(0, sqlValues.length() - 1)
                    + ")";
            try {
                try (Statement stmt = getConnection().createStatement()) {
                    System.out.println("SqlReady:::" + SqlReady);
                    int rows = stmt.executeUpdate(SqlReady);
                    getConnection().commit();
                }
                return true;
            } catch (SQLException e) {
            }
        } catch (SQLException e) {
        }
        return false;
    }

    /*to Update data for the object reflect DB 
     * 
     * 
     * 
     * 
     * tableName is the table used in DB
     * obj is the object of the class reflect the Data for table
     */
    private String getTableName(Object obj) {
        Object objInstance = new Object();
        try {
            objInstance = obj.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
        }
        Table annotationTable = objInstance.getClass().getAnnotation(Table.class);
        String tableName = annotationTable.tableName();

        return tableName;
    }

    public boolean update( Object obj) {
        Object objInstance = new Object();
        Statement statement;
        List<String> pk = new ArrayList();
        String pkColumns = "";

        try {
            objInstance = obj.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
        }

        String tableName = this.getTableName(obj);
        try {
            statement = getConnection().createStatement();
            // ResultSet rs =
            // getConnection().getMetaData().getExportedKeys(null,null,
            // tableName);
            ResultSet columnRs = getConnection().getMetaData().getColumns(null,
                    null, tableName, null);

            String sqlCheck = "Select * from " + tableName;
            String sqlWhere = " where 1=1 and";

            Field[] flds = objInstance.getClass().getDeclaredFields();

            while (columnRs.next()) {
                for (Field fil : flds) {
                    fil.setAccessible(true);
                    if (columnRs
                            .getString("COLUMN_NAME")
                            .replace("_", "")
                            .equalsIgnoreCase(
                                    fil.getName().replace("_", ""))) {
                        Annotation[] ann = fil.getAnnotations();
                        for (Annotation ann1 : ann) {
                            if (ann1.toString().endsWith("FieldPK()")) {
                                sqlWhere = sqlWhere
                                        + " " + columnRs.getString("COLUMN_NAME")
                                        + " = ";
                                if (fil.get(obj) != null) {
                                    if (fil.getType() == Date.class) {

                                        Date value = (Date) fil.get(obj);

                                        sqlWhere = sqlWhere + "'"
                                                + (value.getYear() + 1900)
                                                + "-" + (value.getMonth() + 1)
                                                + "-" + value.getDate()
                                                + "' and";
                                    } else if (fil.getType() == String.class) {
                                        String value = (String) fil
                                                .get(obj);

                                        sqlWhere = sqlWhere + "'" + value
                                                + "' and";
                                    } else {

                                        Object value = fil.get(obj);

                                        sqlWhere = sqlWhere + "'" + value
                                                + "' and";
                                    }

                                } else {

                                    sqlWhere = sqlWhere + "NULL and";
                                }
                            }
                        }

                    }
                }
            }

            columnRs = getConnection().getMetaData().getColumns(null,
                    null, tableName, null);
            sqlWhere = sqlWhere.substring(0, sqlWhere.length() - 3);
            if (!getData(sqlCheck + sqlWhere).iterator().hasNext()) {
                insert(obj);
            } else {
                String sqlUpdate = "Update " + tableName + " set ";
                //columnRs.beforeFirst();
                while (columnRs.next()) {

                    flds = objInstance.getClass().getDeclaredFields();

                    sqlUpdate = sqlUpdate + columnRs.getString("COLUMN_NAME")
                            + "=";
                    for (Field fild : flds) {
                        fild.setAccessible(true);
                        if (columnRs
                                .getString("COLUMN_NAME")
                                .replace("_", "")
                                .equalsIgnoreCase(
                                        fild.getName().replace("_", ""))) {
                            if (fild.get(obj) != null) {
                                if (fild.getType() == Date.class) {

                                    Date value = (Date) fild.get(obj);
                                    sqlUpdate = sqlUpdate + "'"
                                            + (value.getYear() + 1900) + "-"
                                            + (value.getMonth() + 1) + "-"
                                            + value.getDate() + "',";
                                } else if (fild.getType() == String.class) {
                                    String value = (String) fild.get(obj);
                                    sqlUpdate = sqlUpdate + "'" + value + "',";
                                } else {

                                    Object value = fild.get(obj);
                                    sqlUpdate = sqlUpdate + "'" + value + "',";

                                }

                            } else {

                                sqlUpdate = sqlUpdate + "NULL ,";
                            }
                        }
                    }
                }

                try {
                    try (Statement stmt = getConnection().createStatement()) {
                        int rows = stmt.executeUpdate(sqlUpdate.substring(0,
                                sqlUpdate.length() - 1) + sqlWhere);
                        getConnection().commit();
                    }
                    return true;
                } catch (SQLException e) {
                }

            }

        } catch (SQLException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            e.getMessage();
        }

        return false;

    }

    public boolean delete( Object obj) {
        Object objInstance = new Object();
        Statement statement;

        try {
            objInstance = obj.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
        }

        try {
            String tableName = this.getTableName(obj);
            Field[] flds = objInstance.getClass().getDeclaredFields();
            ResultSet columnRs = getConnection().getMetaData().getColumns(null,
                    null, tableName, null);
            String sqlWhere = " where 1=1 and";
            while (columnRs.next()) {
                for (Field flid : flds) {
                    flid.setAccessible(true);
                    if (columnRs
                            .getString("COLUMN_NAME")
                            .replace("_", "")
                            .equalsIgnoreCase(
                                    flid.getName().replace("_", ""))) {
                        Annotation[] ann = flid.getAnnotations();
                        for (Annotation ann1 : ann) {
                            if (ann1.toString().endsWith("FieldPK()")) {
                                sqlWhere = sqlWhere
                                        + " " + columnRs.getString("COLUMN_NAME")
                                        + " = ";
                                if (flid.get(obj) != null) {
                                    if (flid.getType() == Date.class) {

                                        Date value = (Date) flid.get(obj);

                                        sqlWhere = sqlWhere + "'"
                                                + (value.getYear() + 1900)
                                                + "-" + (value.getMonth() + 1)
                                                + "-" + value.getDate()
                                                + "' and";
                                    } else if (flid.getType() == String.class) {
                                        String value = (String) flid
                                                .get(obj);

                                        sqlWhere = sqlWhere + "'" + value
                                                + "' and";
                                    } else {

                                        Object value = flid.get(obj);

                                        sqlWhere = sqlWhere + "'" + value
                                                + "' and";
                                    }

                                } else {

                                    sqlWhere = sqlWhere + "NULL and";
                                }
                            }
                        }

                    }
                }
            }

            try (Statement stmt = getConnection().createStatement()) {
                int rows = stmt.executeUpdate("Delete from " + tableName + sqlWhere.substring(0, sqlWhere.length() - 3));
                getConnection().commit();
            }
            return true;
        } catch (SQLException | IllegalArgumentException | IllegalAccessException e) {
        }

        return false;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Connection getConnection() {
        return connection;
    }

    public int autoIncrement(String columnName, String tableName) {
        String sqlStamtement = "select nvl(max(" + columnName + "),0)+1 ID from  " + tableName;
        System.out.println("autoIncrement::" + sqlStamtement);
        List list = this.getData(sqlStamtement);
        Map map = (Map) list.get(0);
        Integer id = new Integer((String) map.get("ID"));
        System.out.println("autoIncrementId::" + id);
        return id;
    }
}
