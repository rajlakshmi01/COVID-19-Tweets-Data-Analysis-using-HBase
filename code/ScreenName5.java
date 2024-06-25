
//Task-5
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class ScreenName5 {
    public static String Table_Name = "covid19Twitter02";
    
    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        @SuppressWarnings({ "deprecation", "resource" })
        HTable hTable = new HTable(conf, Table_Name);
        
        // Define the filter to find users with special characters in "Screen Name"
        SingleColumnValueFilter NameFilter = new SingleColumnValueFilter(
            Bytes.toBytes("Users"),
            Bytes.toBytes("user_name"),
            CompareOp.EQUAL,
            new RegexStringComparator(".*[^a-zA-Z0-9\\s].*")
        );
       // System.out.println(Bytes.toBytes(".*[^a-zA-Z0-9\\s].*"));
        
        // Define the filter to find verified users
        SingleColumnValueFilter verifiedFilter = new SingleColumnValueFilter(
        	    Bytes.toBytes("Users"),
        	    Bytes.toBytes("user_verified"),
        	    CompareOp.EQUAL,
        	    new RegexStringComparator("(?i)true|True")
        	);
        
        Scan scan1 = new Scan();
        scan1.setFilter(NameFilter);
        
        Scan scan2 = new Scan();
        scan2.setFilter(verifiedFilter);
        
        // Now we extract the results
        ResultScanner scanner1 = hTable.getScanner(scan1);
        ResultScanner scanner2 = hTable.getScanner(scan2);
        
        int spCharUsersCount = 0;
        List<String> spCharNames = new ArrayList<>();
        for (Result result = scanner1.next(); result != null; result = scanner1.next()) {
        	byte[] NameBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
            String Name = Bytes.toString(NameBytes);
            spCharNames.add(Name);
            spCharUsersCount++;
        }
        
        int verifiedUsersSpCount = 0;
        for (Result result = scanner2.next(); result != null; result = scanner2.next()) {
            byte[] NameBytes = result.getValue(Bytes.toBytes("Users"), Bytes.toBytes("user_name"));
            String Name = Bytes.toString(NameBytes);
            if (Name.matches(".*[^a-zA-Z0-9\\s].*")) {
            	System.out.println(Name);
            	verifiedUsersSpCount++;
            }
        }
        System.out.println("Names of users with special characters in Name:");
        for (String name : spCharNames) {
            System.out.println(name);
        }
        System.out.println("Users with special characters in Name: " + spCharUsersCount);
        System.out.println("Verified users with special characters in Name: " + verifiedUsersSpCount);
    }
}
