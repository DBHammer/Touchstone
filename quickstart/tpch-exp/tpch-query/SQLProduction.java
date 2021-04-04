import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author wangqingshuai
 */
public class SQLProduction {
    public static final Pattern p = Pattern.compile("(#[\\d,]+#)");

    public static void main(String[] args) {
        String parametersPath = args[0];
        String sqlIndex = args[1];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Map<Integer, String[]> parameterMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(parametersPath)))) {
            String inputLine;
            while ((inputLine = br.readLine()) != null) {
                if (!inputLine.contains("Parameter")) {
                    continue;
                }
                String[] arr = inputLine.split("id=|, values=|, cardinality=");
                int id = Integer.parseInt(arr[1]);
                String[] values = arr[2].substring(1, arr[2].length() - 1).replaceAll(" ", "").split(",");
                parameterMap.put(id, values);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sqlIndex + ".sql")))) {
            String inputLine;
            while ((inputLine = br.readLine()) != null) {
                Matcher m = p.matcher(inputLine);
                while (m.find()) {
                    String paraInfo = m.group();
                    String[] arr = paraInfo.substring(1, paraInfo.length() - 1).split(",");
                    int id = Integer.parseInt(arr[0]);
                    int index = Integer.parseInt(arr[1]);
                    String paraStr = parameterMap.get(id)[index];
                    if ("0".equals(arr[2])) {
                        inputLine = inputLine.replaceAll(paraInfo, paraStr);
                    } else if ("1".equals(arr[2])) {
                        inputLine = inputLine.replaceAll(paraInfo, sdf.format(new Date((long) Double.parseDouble(paraStr))));
                    }
                }
                System.out.println(inputLine);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

