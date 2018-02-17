package com.bluecast.bluevigil.model;

import java.io.Serializable;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;



public class KeyField implements Serializable{

@SerializedName("backEndField")
@Expose
private String backEndField;

public String getBackEndField() {
return backEndField;
}

public void setBackEndField(String backEndField) {
this.backEndField = backEndField;
}

}