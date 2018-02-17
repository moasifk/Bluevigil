package com.bluecast.bluevigil.model.QueryFields;

import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class QueryFieldMapping {

@SerializedName("QueryFields")
@Expose
private List<QueryField> queryFields = new ArrayList<QueryField>();

public List<QueryField> getQueryFields() {
return queryFields;
}

public void setQueryFields(List<QueryField> queryFields) {
this.queryFields = queryFields;
}

}
