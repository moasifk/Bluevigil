package com.bluecast.bluevigil.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class FieldMapping implements Serializable{

@SerializedName("logFileName")
@Expose
private String logFileName;
@SerializedName("delimitter")
@Expose
private String delimitter;
@SerializedName("hbaseTable")
@Expose
private String hbaseTable;
@SerializedName("hbaseColumnFamily")
@Expose
private String hbaseColumnFamily;
@SerializedName("kafkaTopic")
@Expose
private String kafkaTopic;
@SerializedName("keyFields")
@Expose
private List<KeyField> keyFields = new ArrayList<KeyField>();
@SerializedName("mapping")
@Expose
private List<Mapping> mapping = new ArrayList<Mapping>();

public FieldMapping() {
	
}
public String getLogFileName() {
return logFileName;
}

public void setLogFileName(String logFileName) {
this.logFileName = logFileName;
}

public String getDelimitter() {
return delimitter;
}

public void setDelimitter(String delimitter) {
this.delimitter = delimitter;
}

public String getHbaseTable() {
return hbaseTable;
}

public void setHbaseTable(String hbaseTable) {
this.hbaseTable = hbaseTable;
}

public String getHbaseColumnFamily() {
return hbaseColumnFamily;
}

public void setHbaseColumnFamily(String hbaseColumnFamily) {
this.hbaseColumnFamily = hbaseColumnFamily;
}

public String getKafkaTopic() {
return kafkaTopic;
}

public void setKafkaTopic(String kafkaTopic) {
this.kafkaTopic = kafkaTopic;
}

public List<KeyField> getKeyFields() {
return keyFields;
}

public void setKeyFields(List<KeyField> keyFields) {
this.keyFields = keyFields;
}

public List<Mapping> getMapping() {
return mapping;
}

public void setMapping(List<Mapping> mapping) {
this.mapping = mapping;
}

}