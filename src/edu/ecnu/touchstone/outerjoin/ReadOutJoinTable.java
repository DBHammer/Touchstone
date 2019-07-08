package edu.ecnu.touchstone.outerjoin;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;

public class ReadOutJoinTable {

    private DataInputStream joinTableInputStream;
    private Long[] primaryKeys;
    public ReadOutJoinTable(String joinTableOutPath) throws IOException {
        joinTableInputStream = new DataInputStream(new BufferedInputStream(
                new FileInputStream(new File(joinTableOutPath))));
        primaryKeys=new Long[joinTableInputStream.read()];
    }

    public Pair<Integer,Long[]> read() throws IOException {
        int status=joinTableInputStream.read();
        for (int i = 0; i < primaryKeys.length; i++) {
            primaryKeys[i]=joinTableInputStream.readLong();
        }
        return new ImmutablePair<>(status,primaryKeys);
    }

    public void close() throws IOException {
        joinTableInputStream.close();
    }

}
