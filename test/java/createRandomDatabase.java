import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class createRandomDatabase {
    private static final int PART_SIZE = 10000;
    private static final int PRODUCT_SIZE = 40000;

    public static void main(String[] args) throws IOException {
        int sf = 1;
        Random random = new Random();
        ArrayList<String> records = new ArrayList<>();
        //Part Supplier
        RandomString supplier = new RandomString(100, 4, 8);
        //Part Type
        RandomString type = new RandomString(100, 4, 8);

        //generate records
        for (int lineNum = 0; lineNum < PART_SIZE; lineNum++) {
            records.add(',' + supplier.getString() + ',' + type.getString() + ',' +
                    random.nextInt(10000) + "\n");
        }
        //write part table
        for (int i = 0; i < sf; i++) {
            BufferedWriter partTableOut = new BufferedWriter(new FileWriter("myData/part_" + i+".txt"));
            for (int lineNum = 0; lineNum < PART_SIZE; lineNum++) {
                partTableOut.write((lineNum + i * PART_SIZE) + records.get(lineNum));
            }
            partTableOut.close();
        }

        //Product Kind
        RandomString kind = new RandomString(100, 4, 8);
        //generate records and fks
        records.clear();
        ArrayList<Integer> fks = new ArrayList<>();
        for (int lineNum = 0; lineNum < PRODUCT_SIZE; lineNum++) {
            records.add(',' + kind.getString() + ',' + random.nextInt(1000000) + '\n');
            fks.add(random.nextInt(PART_SIZE/10000));
        }

        //write product table
        for (int i = 0; i < sf; i++) {
            BufferedWriter productTableOut = new BufferedWriter(new FileWriter("myData/product_" + i+".txt"));
            for (int lineNum = 0; lineNum < PRODUCT_SIZE; lineNum++) {
                productTableOut.write(String.valueOf(lineNum + i * PRODUCT_SIZE) + ',' + (fks.get(lineNum) +
                        i * PART_SIZE) + records.get(lineNum));
            }
            productTableOut.close();
        }
    }
}

class RandomString {
    private ArrayList<String> seeds;
    private final static Random R = new Random();

    private String randomString(int min, int range) {
        int length = min + R.nextInt(range);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < length; i++) {
            result.append((char) (R.nextInt(26) + 97));
        }
        return result.toString();
    }

    RandomString(int uniqueNum, int min, int max) {
        seeds = new ArrayList<>();
        int range = max - min;
        for (int i = 0; i < uniqueNum; i++) {
            seeds.add(randomString(min, range));
        }
    }

    public String getString() {
        return seeds.get(R.nextInt(seeds.size()));
    }
}
