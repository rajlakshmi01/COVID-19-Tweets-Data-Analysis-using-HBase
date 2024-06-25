//Task-3
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InsertData {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("covid19Twitter"))) {

            byte[] columnFamily = Bytes.toBytes("Users");
            byte[] columnQualifier = Bytes.toBytes("user_location");

            Scan scan = new Scan();
            scan.addColumn(columnFamily, columnQualifier);

            ResultScanner scanner = table.getScanner(scan);

            Map<String, Integer> locationTweetCount = new HashMap<>();

            for (Result result : scanner) {
                byte[] locationBytes = result.getValue(columnFamily, columnQualifier);

                if (locationBytes != null) {
                    String location = Bytes.toString(locationBytes);
                    locationTweetCount.put(location, locationTweetCount.getOrDefault(location, 0) + 1);
                }
            }

            for (Map.Entry<String, Integer> entry : locationTweetCount.entrySet()) {
                System.out.println("Location: " + entry.getKey() + ", Tweet Count: " + entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();}
 
    }
}
// to find user_location code 

