
package com.bluevigil.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class FieldMapping {

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

	public void setRequired(boolean isRequired) {
		this.isRequired = isRequired;
	}

	public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("backEndField", backEndField).append("order", order).append("isRequired", isRequired).append("label", label).toString();
    }

}
