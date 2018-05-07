
package com.bluevigil.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

public class FieldMapping implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@SerializedName("BackEndField")
    @Expose
    private String backEndField;
    @SerializedName("Order")
    @Expose
    private int order;
    @SerializedName("IsRequired")
    @Expose
    private boolean isRequired;
    @SerializedName("Label")
    @Expose
    private String label;
    @SerializedName("DerivedType")
    @Expose
    private String derivedType;
    @SerializedName("IsDerived")
    @Expose
    private boolean isDerived;


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

    public boolean isRequired() {
		return isRequired;
	}

	public void setIsRequired(boolean isRequired) {
		this.isRequired = isRequired;
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
    public boolean isDerived() {
		return isDerived;
	}

	public void setRequired(boolean isDerived) {
		this.isDerived = isDerived;
	}

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("backEndField", backEndField).append("order", order).append("isRequired", isRequired).append("label", label).append("derivedType", derivedType).append("isDerived", isDerived).toString();
    }

}