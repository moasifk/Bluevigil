package com.bluevigil.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

public class DerivedFieldMapping implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@SerializedName("DerivedField")
    @Expose
    private String derivedField;
	@SerializedName("BackEndField")
    @Expose
    private String backEndField;
    @SerializedName("Order")
    @Expose
    private int order;
    @SerializedName("Label")
    @Expose
    private String label;
	@SerializedName("DerivedType")
    @Expose
    private String derivedType;

	public String getDerivedField() {
        return derivedField;
    }

    public void setDerivedField(String derivedField) {
        this.derivedField = derivedField;
    }
	public String getBackEndField() {
        return backEndField;
    }

    public void setBackEndField(String backEndField) {
        this.backEndField = backEndField;
    }

    public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}
	public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }
    public String getDerivedType() {
        return derivedType;
    }

    public void setDerivedType(String derivedType) {
        this.derivedType = derivedType;
    }

	@Override
    public String toString() {
        return new ToStringBuilder(this).append("derivedField", derivedField).append("backEndField", backEndField).append("order", order).append("label", label).append("derivedType", derivedType).toString();
    }

}
