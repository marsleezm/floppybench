package floppystore.application.ycsb;

import java.util.HashMap;
import java.util.Map;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;

public class FloppyWorkload extends Workload {

	@Override
	public boolean doInsert(DB db, Object threadstate) {
		// TODO Auto-generated method stub
		HashMap<String, ByteIterator> params = new HashMap<String, ByteIterator>(); 
		((FloppyClient)db).insert("", "whatever", params);
		return false;
	}

	@Override
	public boolean doTransaction(DB db, Object threadstate) {
		// TODO Auto-generated method stub
		return false;
	}

}
