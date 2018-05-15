package com.bluevigil.analytics.model;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class JsonParsedfields implements Serializable{
	private static final long serialVersionUID = 1L;
	@SerializedName("FieldName")
    @Expose
    private String fieldName;
	@SerializedName("BackEndField")
    @Expose
    private String backEndField;
	
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}	
	public String getBackEndField() {
		return backEndField;
	}
	public void setBackEndField(String backEndField) {
		this.backEndField = backEndField;
	}
	
	@Override
    public String toString() {
        return new ToStringBuilder(this).append("fieldName", fieldName).append("backEndField", backEndField).toString();
    }
	


}
