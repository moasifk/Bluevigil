
package com.bluevigil.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class RowKeyField {

    @SerializedName("BackEndField")
    @Expose
    private String backEndField;
    @SerializedName("Order")
    @Expose
    private int order;

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

	@Override
    public String toString() {
        return new ToStringBuilder(this).append("backEndField", backEndField).append("order", order).toString();
    }

}
