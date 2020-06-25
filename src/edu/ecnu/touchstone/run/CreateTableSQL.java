package edu.ecnu.touchstone.run;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CreateTableSQL {


    public static String getSql(String tableInfo) {
        tableInfo = tableInfo.substring(2, tableInfo.length() - 1);
        String[] columns = tableInfo.split(";");
        StringBuilder sql = new StringBuilder("CREATE TABLE " + columns[0] + "(");
        int i = 2;
        for (; i < columns.length; i++) {
            if (columns[i].startsWith("P(")) {
                break;
            }
            String[] columnInfo = columns[i].split(",");
            if (columnInfo[1].contains("integer")) {
                sql.append(columnInfo[0]).append(" int").append(",");
            } else if (columnInfo[1].contains("varchar")) {
                sql.append(columnInfo[0]).append(" varchar(30)").append(",");
            } else if(columnInfo[1].contains("date")){
                sql.append(columnInfo[0]).append(" datetime").append(",");
            }
        }
        sql.append("PRIMARY KEY (").append(columns[i].substring(2)).append(");\n");
        String tableName = columns[0];
        sql.append("LOAD DATA LOCAL INFILE 'data/").append(tableName).append("_0.txt' INTO TABLE ").append(tableName).append(" FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';");
        return sql.toString();
    }

    public static void main(String[] args) throws IOException {
        System.out.println("drop database if exists beike;");
        System.out.println("create database beike;");
        System.out.println("use beike;");
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/wangqingshuai/Github/Touchstone/running examples/beike/table.conf"));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if(line.startsWith("T[")){
                System.out.println(getSql(line));
            }
        }

    }
}
