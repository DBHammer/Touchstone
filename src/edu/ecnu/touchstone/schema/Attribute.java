package edu.ecnu.touchstone.schema;

import edu.ecnu.touchstone.datatype.*;

import java.io.Serializable;

public class Attribute implements Serializable {

    private static final long serialVersionUID = 1L;

    private String attrName = null;
    private String dataType = null;
    private int index;

    // information of basic data characteristics
    private TSDataTypeInfo dataTypeInfo = null;

    public Attribute(String attrName, String dataType, int index, TSDataTypeInfo dataTypeInfo) {
        super();
        this.attrName = attrName;
        this.dataType = dataType;
        this.dataTypeInfo = dataTypeInfo;
        this.index = index;
    }

    public Attribute(Attribute attribute) {
        super();
        this.attrName = attribute.attrName;
        this.dataType = attribute.dataType;
        this.index = attribute.index;
        switch (this.dataType) {
            case "integer":
                this.dataTypeInfo = new TSInteger((TSInteger) attribute.dataTypeInfo);
                break;
            case "real":
                this.dataTypeInfo = new TSReal((TSReal) attribute.dataTypeInfo);
                break;
            case "decimal":
                this.dataTypeInfo = new TSDecimal((TSDecimal) attribute.dataTypeInfo);
                break;
            case "date":
                this.dataTypeInfo = new TSDate((TSDate) attribute.dataTypeInfo);
                break;
            case "datetime":
                this.dataTypeInfo = new TSDateTime((TSDateTime) attribute.dataTypeInfo);
                break;
            case "varchar":
                this.dataTypeInfo = new TSVarchar((TSVarchar) attribute.dataTypeInfo);
                break;
            case "bool":
                this.dataTypeInfo = new TSBool((TSBool) attribute.dataTypeInfo);
                break;
        }
    }

    public String geneData() {
        return dataTypeInfo.geneData().toString();
    }

    public String getAttrName() {
        return attrName;
    }

    public String getDataType() {
        return dataType;
    }

    public TSDataTypeInfo getDataTypeInfo() {
        return dataTypeInfo;
    }

    // automatically acquire the data characteristics -- DBStatisticsCollector
    public void setDataTypeInfo(TSDataTypeInfo dataTypeInfo) {
        this.dataTypeInfo = dataTypeInfo;
    }

    @Override
    public String toString() {
        return "\n\tAttribute [attrName=" + attrName + ", dataType=" + dataType + ", dataTypeInfo=" + dataTypeInfo + "]";
    }

    public int getIndex() {
        return index;
    }
}
