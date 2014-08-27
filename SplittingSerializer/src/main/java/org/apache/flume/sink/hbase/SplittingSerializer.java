package org.apache.flume.sink.hbase;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

public class SplittingSerializer implements AsyncHbaseEventSerializer {
	  private byte[] table;
	  private byte[] colFam;
	  private Event currentEvent;
	  private byte[][] columnNames;
	  private final List<PutRequest> puts = new ArrayList<PutRequest>();
	  private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
	  private byte[] currentRowKey;
	  private final byte[] eventCountCol = "eventCount".getBytes();

	  @Override
	  public void initialize(byte[] table, byte[] cf) {
	    this.table = table;
	    this.colFam = cf;
	    
	  }

	  @Override
	  public void setEvent(Event event) {
	    // Set the event and verify that the rowKey is not present
	    this.currentEvent = event;
	    String rowKeyStr = currentEvent.getHeaders().get("id");
	    if (rowKeyStr == null) {
	      throw new FlumeException("No row key found in headers!");
	    }
	    currentRowKey = Bytes.toBytes(rowKeyStr);
	  }

	  @Override
	  public List<PutRequest> getActions() {
	    // Split the event body and get the values for the columns
	    //String rawJson = new String(currentEvent.getBody(),Charset.forName("UTF-8"));
	    //String rawJson = currentEvent.getBody();
	    puts.clear();
	    PutRequest req = new PutRequest(table, currentRowKey, colFam,
	              Bytes.toBytes("json"), currentEvent.getBody());
	    puts.add(req);
	    
	    return puts;
	  }

	  @Override
	  public List<AtomicIncrementRequest> getIncrements() {
	    incs.clear();
	    //Increment the number of events received
	    incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
	    return incs;
	  }

	  @Override
	  public void cleanUp() {
	    table = null;
	    colFam = null;
	    currentEvent = null;
	    columnNames = null;
	    currentRowKey = null;
	  }

	  @Override
	  public void configure(Context context) {
	    //Get the column names from the configuration
	    String cols = new String(context.getString("columns"));
	    String[] names = cols.split(",");
	    byte[][] columnNames = new byte[names.length][];
	    int i = 0;
	    for(String name : names) {
	      columnNames[i++] = name.getBytes();
	    }
	  }

	  @Override
	  public void configure(ComponentConfiguration conf) {
	  }
	}