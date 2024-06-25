import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class userverified {
    public static String Table_Name = "covid19Twitter";

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings({"deprecation", "resource"})
        HTable hTable = new HTable(conf, Table_Name);

        Scan scan1 = new Scan();

        // Now we extract the results
        ResultScanner scanner1 = hTable.getScanner(scan1);

        Map<String, Integer> usersCreatedByYear = new HashMap<>();
        Map<String, Integer> usersCreatedIn2020ByMonth = new HashMap<>();

        for (Result result : scanner1) {
            byte[] userCreatedBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_created"));
            if (userCreatedBytes != null) {
                String userCreated = Bytes.toString(userCreatedBytes);
                String[] parts = userCreated.split("-");

                if (parts.length >= 1) {
                    String year = parts[0];
                    usersCreatedByYear.put(year, usersCreatedByYear.getOrDefault(year, 0) + 1);

                    if (parts.length >= 2 && year.equals("2020")) {
                        String month = parts[1];
                        usersCreatedIn2020ByMonth.put(month, usersCreatedIn2020ByMonth.getOrDefault(month, 0) + 1);
                    }
                }
            }
        }

        // Print the number of users created in each year
        System.out.println("Users Created by Year:");
        for (Map.Entry<String, Integer> entry : usersCreatedByYear.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue() + " users created");
        }

        // Print the number of users created in each month of 2020
        System.out.println("Users Created in Each Month of 2020:");
        for (Map.Entry<String, Integer> entry : usersCreatedIn2020ByMonth.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue() + " users created");
        }
    }
}
