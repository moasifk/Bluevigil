package com.bluecast.bluevigil.model;

import java.io.Serializable;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Mapping implements Serializable{

/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
@SerializedName("backEndField")
@Expose
private String backEndField;
@SerializedName("hbaseField")
@Expose
private String hbaseField;
@SerializedName("type")
@Expose
private String type;
@SerializedName("label")
@Expose
private String label;

public String getBackEndField() {
return backEndField;
}

public void setBackEndField(String backEndField) {
this.backEndField = backEndField;
}

public String getHbaseField() {
return hbaseField;
}

public void setHbaseField(String hbaseField) {
this.hbaseField = hbaseField;
}

public String getType() {
return type;
}

public void setType(String type) {
this.type = type;
}

public String getLabel() {
return label;
}

public void setLabel(String label) {
this.label = label;
}

}