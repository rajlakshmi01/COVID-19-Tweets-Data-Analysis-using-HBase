//Task-6
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class PopularUsers2 {
    public static String Table_Name = "covid19Twitter02";

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings({"deprecation", "resource"})
        HTable hTable = new HTable(conf, Table_Name);

        Scan scan1 = new Scan();

        // Now we extract the results
        ResultScanner scanner1 = hTable.getScanner(scan1);

        int popularUsersCount = 0;
        int verifiedCount = 0;
        int unverifiedCount = 0;

        for (Result result : scanner1) {
            byte[] followersBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"));
            if (followersBytes != null) {
                String followersString = Bytes.toString(followersBytes);

                // Convert the followersString to integer
                int followersCount;
                try {
                    followersCount = Integer.parseInt(followersString);

                    if (followersCount > 100000) { // Checking if value is greater than 99999 (6 digit)
                        byte[] verifiedBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"));
                        if (verifiedBytes != null) {
                            String verifiedString = Bytes.toString(verifiedBytes);

                            // Convert to uppercase for case-insensitive comparison
                            verifiedString = verifiedString.toUpperCase();

                            if (verifiedString.equals("TRUE")) {
                                verifiedCount++;
                            } else {
                                unverifiedCount++;
                            }
                        }

                        byte[] nameBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
                        String name = Bytes.toString(nameBytes);

                        System.out.println(name);
                        popularUsersCount++;
                    }
                } catch (NumberFormatException e) {
                    // Handle if the conversion to integer fails (e.g., non-numeric data in 'user_followers')
                	continue;
                }
            }
        }

        System.out.println("Popular users with more than 6 digits in User Followers: " + popularUsersCount);
        System.out.println("Popular users who are Verified: " + verifiedCount);
        System.out.println("Popular users who are UnVerified: " + unverifiedCount);
    }
}
