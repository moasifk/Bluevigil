package com.bluecast.bluevigil.model.QueryFields;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class QueryField {

@SerializedName("tableName")
@Expose
private String tableName;
@SerializedName("columnQualifierName")
@Expose
private String columnQualifierName;
@SerializedName("sqlConstructs")
@Expose
private String sqlConstructs;
@SerializedName("columnQualifierValue")
@Expose
private String columnQualifierValue;
@SerializedName("joinConstruct")
@Expose
private String joinConstruct;

public String getTableName() {
return tableName;
}

public void setTableName(String tableName) {
this.tableName = tableName;
}

public String getColumnQualifierName() {
return columnQualifierName;
}

public void setColumnQualifierName(String columnQualifierName) {
this.columnQualifierName = columnQualifierName;
}

public String getSqlConstructs() {
return sqlConstructs;
}

public void setSqlConstructs(String sqlConstructs) {
this.sqlConstructs = sqlConstructs;
}

public String getColumnQualifierValue() {
return columnQualifierValue;
}

public void setColumnQualifierValue(String columnQualifierValue) {
this.columnQualifierValue = columnQualifierValue;
}

public String getJoinConstruct() {
return joinConstruct;
}

public void setJoinConstruct(String joinConstruct) {
this.joinConstruct = joinConstruct;
}

}