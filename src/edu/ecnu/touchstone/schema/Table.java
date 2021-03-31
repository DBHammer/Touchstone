package edu.ecnu.touchstone.schema;

import java.util.List;
import java.util.Map;

public class Table {

    private String tableName = null;
    private long tableSize;

    // support the composite primary key
    private List<String> primaryKey = null;

    private Map<String, Integer> keyIndex = null;

    // support multiple foreign keys
    private List<ForeignKey> foreignKeys = null;

    // 'attributes' doesn't include the key attributes
    private List<Attribute> attributes = null;

    public Table(String tableName, long tableSize, List<String> primaryKey, List<ForeignKey> foreignKeys,
                 List<Attribute> attributes, Map<String, Integer> keyIndex) {
        super();
        this.tableName = tableName;
        this.tableSize = tableSize;
        this.primaryKey = primaryKey;
        this.foreignKeys = foreignKeys;
        this.attributes = attributes;
        this.keyIndex = keyIndex;
    }

    public Map<String, Integer> getKeyIndex() {
        return keyIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableSize() {
        return tableSize;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "\nTable [tableName=" + tableName + ", tableSize=" + tableSize + ", primaryKey="
                + primaryKey + ", foreignKeys=" + foreignKeys + ", \nattributes=" + attributes + "]";
    }
}
