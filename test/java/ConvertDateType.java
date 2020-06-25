import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class ConvertDateType {
    private static Set<Integer> needLine = new HashSet<>(Arrays.asList(6, 7, 8, 12, 13, 14, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 43));
    private static Set<Integer> isDate = new HashSet<>(Arrays.asList(7, 8, 13, 14, 21, 22, 25, 26, 29, 30));
    private static Set<Integer> isLike = new HashSet<>(Collections.singletonList(43));

    public static void main(String[] args) throws IOException {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            StringBuilder str = new StringBuilder();
            String line;
            int i = 1;
            while ((line = bf.readLine()).length() != 0) {
                String temp = line.substring(10);
                String[] arrs = temp.split("=");
                if (needLine.contains(Integer.parseInt(arrs[0]))) {
                    if (isDate.contains(Integer.parseInt(arrs[0]))) {
                        line = "set @value" + i + "='" +
                                sf.format(new Date((long) Double.parseDouble(arrs[1].split("'")[1]))) + "';";
                    } else if (isLike.contains(Integer.parseInt(arrs[0]))) {
                        line = "set @value" + i + "='%" + arrs[1].split("'")[1] + "%';";
                    } else {
                        line = "set @value" + i + "=" + arrs[1];
                    }
                    i++;
                    str.append(line).append('\n');
                }
            }
            System.out.println(str);
        }
    }
}
