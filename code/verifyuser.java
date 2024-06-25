//Task-4
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class InsertData {
    public static String Table_Name = "covid19Twitter";

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings({"deprecation", "resource"})
        HTable hTable = new HTable(conf, Table_Name);

        Scan scan1 = new Scan();

        // Now we extract the results
        ResultScanner scanner1 = hTable.getScanner(scan1);

        int verifiedCount = 0;

        Map<String, Integer> verifiedUsersByYear = new HashMap<>();
        Map<String, Integer> usersCreatedIn2020ByMonth = new HashMap<>();

        for (Result result : scanner1) {
            byte[] verifiedBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"));
            if (verifiedBytes != null) {
                String verifiedString = Bytes.toString(verifiedBytes);

                // Convert to uppercase for case-insensitive comparison
                verifiedString = verifiedString.toUpperCase();

                if (verifiedString.equals("TRUE")) {
                    verifiedCount++;

                    // Extract the user_created date for year and month
                    byte[] userCreatedBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_created"));
                    if (userCreatedBytes != null) {
                        String userCreated = Bytes.toString(userCreatedBytes);
                        String[] parts = userCreated.split("-");

                        if (parts.length >= 1) {
                            String year = parts[0];
                            // Increment the count for verified users by year
                            verifiedUsersByYear.put(year, verifiedUsersByYear.getOrDefault(year, 0) + 1);

                            if (parts.length >= 2 && year.equals("2020")) {
                                String month = parts[1];
                                // Increment the count for users created in each month of 2020
                                usersCreatedIn2020ByMonth.put(month, usersCreatedIn2020ByMonth.getOrDefault(month, 0) + 1);
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Verified Users: " + verifiedCount);

        // Print the number of verified users in each year
        System.out.println("Verified Users by Year:");
        for (Map.Entry<String, Integer> entry : verifiedUsersByYear.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue() + " verified users");
        }

        // Print the number of users created in each month of 2020
        System.out.println("Users Created in Each Month of 2020:");
        for (Map.Entry<String, Integer> entry : usersCreatedIn2020ByMonth.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue() + " users created");
        }
    }
}
