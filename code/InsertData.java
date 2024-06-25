import com.opencsv.*;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.util.Arrays;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool{

	public String Table_Name = "covid19Twitter02";
    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] argv) throws IOException, CsvValidationException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);        
        
        boolean isExists = admin.tableExists(Table_Name);
        
        if(isExists == false) {
	        //create table with column family
	        HTableDescriptor htb=new HTableDescriptor(Table_Name);
	        HColumnDescriptor UserFamily = new HColumnDescriptor("Users");
	        HColumnDescriptor TweetsFamily = new HColumnDescriptor("Tweets");
	        HColumnDescriptor ExamFamily = new HColumnDescriptor("Extra");
	        
	        htb.addFamily(UserFamily);
	        htb.addFamily(TweetsFamily);
	        htb.addFamily(ExamFamily);
	        admin.createTable(htb);
        }
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader("covid19_tweets.csv"))
                .withSkipLines(1) // Skip the header line if needed
                .withCSVParser(new com.opencsv.CSVParserBuilder().withSeparator(',').build())
                .build()) {

            String[] lineArray;
    	    int row_count=0;
    	    int row_skip=0;
    	    //iterate over every line of the input file
    	    while((lineArray = reader.readNext()) != null) {
    	
    	    	row_count++;
    	    
    	    	
    	    	if (lineArray.length !=13) {
    	    		System.err.println("skipping line with insufficient data" + lineArray);
    	    		row_skip++;
    	            continue;
    	        }
    	    	String user_name = lineArray[0];
				String user_location = lineArray[1];
				String user_description = lineArray[2];
				String user_created = lineArray[3];
//				int user_followers = Integer.parseInt(lineArray[4]);
//				int user_friends = Integer.parseInt(lineArray[5]);
//				int user_favourites = Integer.parseInt(lineArray[6]);
				
				String user_followers = lineArray[4];
				String user_friends = lineArray[5];
				String user_favourites =lineArray[6];
				
				String user_verified = lineArray[7];
				String date = lineArray[8];
				String text = lineArray[9];
				String hashtags = lineArray[10];
				String source = lineArray[11];
				String is_retweet = lineArray[12];
                //row_count++;
				// Initialize a Put with row key as tweet_url
				String user_id="N"+row_count;
				Put put = new Put(Bytes.toBytes(user_id));

				// Add column data one by one
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_name"), Bytes.toBytes(user_name));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_location"), Bytes.toBytes(user_location));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_description"), Bytes.toBytes(user_description));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_created"), Bytes.toBytes(user_created));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_followers"), Bytes.toBytes(user_followers));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_friends"), Bytes.toBytes(user_friends));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_favourites"), Bytes.toBytes(user_favourites));
				put.addColumn(Bytes.toBytes("Users"), Bytes.toBytes("user_verified"), Bytes.toBytes(user_verified));

				put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("Text"), Bytes.toBytes(text));
				put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("Tweet_Date"), Bytes.toBytes(date));

				put.addColumn(Bytes.toBytes("Extra"), Bytes.toBytes("hashtags"), Bytes.toBytes(hashtags));
				put.addColumn(Bytes.toBytes("Extra"), Bytes.toBytes("source"), Bytes.toBytes(source));
				put.addColumn(Bytes.toBytes("Extra"), Bytes.toBytes("is_retweet"), Bytes.toBytes(is_retweet));
	            
	            //add the put in the table
    	    	HTable hTable = new HTable(conf, Table_Name);
    	    	hTable.put(put);
    	    	hTable.close();    
	    	}
    	    System.out.println("Inserted " + (row_count-row_skip) + " Inserted");
    	    
	    } catch (FileNotFoundException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } 

      return 0;
   }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);}
}
