//Task-2
package com.opencsv;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.List;

public class Versions{

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("covid19Twitter"))) {

            // Row key for the entry you want to work with
            String rowKey = "user_name";
            byte[] columnFamily = Bytes.toBytes("Users");
            byte[] columnQualifier = Bytes.toBytes("User Bio");

            // Task 5: Enable versioning for the "Users" column family
            HTableDescriptor tableDescriptor = table.getTableDescriptor();
            HColumnDescriptor usersColumnFamily = tableDescriptor.getFamily(columnFamily);
            usersColumnFamily.setMaxVersions(5); // Specify the number of versions to retain
            connection.getAdmin().modifyTable(TableName.valueOf("covid19Twitter"), tableDescriptor);

            // Task 6: Put multiple versions of "User Bio"
            Put put1 = new Put(Bytes.toBytes(rowKey));
            put1.addColumn(columnFamily, columnQualifier, 1, Bytes.toBytes("Version 1 User Bio"));

            Put put2 = new Put(Bytes.toBytes(rowKey));
            put2.addColumn(columnFamily, columnQualifier, 2, Bytes.toBytes("Version 2 User Bio"));

            Put put3 = new Put(Bytes.toBytes(rowKey));
            put3.addColumn(columnFamily, columnQualifier, 3, Bytes.toBytes("Version 3 User Bio"));

            table.put(put1);
            table.put(put2);
            table.put(put3);

            // Retrieve and print the versions of "User Bio" for the given row key
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(columnFamily, columnQualifier);
            get.setMaxVersions(5);

            Result result = table.get(get);
            List<Cell> cells = result.getColumnCells(columnFamily, columnQualifier);

            for (Cell cell : cells) {
                byte[] cellValue = CellUtil.cloneValue(cell);
                long timestamp = cell.getTimestamp();
                String userBio = Bytes.toString(cellValue);

                System.out.println("User Bio (Timestamp " + timestamp + "): " + userBio);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
