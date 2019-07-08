package edu.ecnu.touchstone.outerjoin;

import java.io.*;

public class WriteOutJoinTable {

    private DataOutputStream joinTableOutputStream;
    public WriteOutJoinTable(String joinTableOutPath,int primaryKeyLength) throws IOException {
        joinTableOutputStream = new DataOutputStream(new BufferedOutputStream(
                new FileOutputStream(new File(joinTableOutPath))));
        joinTableOutputStream.write(primaryKeyLength);
    }

    public void write(int status,long[] primaryKey) throws IOException {
        joinTableOutputStream.write(status);
        for (long l : primaryKey) {
            joinTableOutputStream.writeLong(l);
        }
    }

    public void close() throws IOException {
        joinTableOutputStream.close();
    }
}
