package clocksi.application.ycsb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import clocksi.application.ycsb.ClocksiPB.FpbCommitTxReq;
import clocksi.application.ycsb.ClocksiPB.FpbCommitTxResp;
import clocksi.application.ycsb.ClocksiPB.FpbReadReq;
import clocksi.application.ycsb.ClocksiPB.FpbReadResp;
import clocksi.application.ycsb.ClocksiPB.FpbStartTxReq;
import clocksi.application.ycsb.ClocksiPB.FpbStartTxResp;
import clocksi.application.ycsb.ClocksiPB.FpbUpdateReq;
import clocksi.application.ycsb.ClocksiPB.FpbUpdateResp;
import clocksi.application.ycsb.ClocksiPB.TxId;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.google.protobuf.ByteString;

public class ClocksiClient extends DB {
	
    private ClocksiConnection connection = null;
    private String addr = "127.0.0.1";
    private Integer port = 8087;
    private String server = "floppy";
    
    private static final Integer MSG_StartTXReq = 94;
    private static final Integer MSG_StartTXResp = 95;
    private static final Integer MSG_UpdateReq = 96;
    private static final Integer MSG_UpdateResp = 97;
    private static final Integer MSG_ReadReq = 98;
    private static final Integer MSG_ReadResp = 99;
    private static final Integer MSG_CommitTxReq = 100;
    private static final Integer MSG_CommitTxResp = 101;
	
    
    public void init() throws DBException {
        try {
        	System.out.println("dfaConnected!");
        	connection = new ClocksiConnection(addr, port);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
    }

	
	@Override
	// TODO Table is actually type!
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			System.out.println("Read");
			FpbStartTxReq startTxReq = FpbStartTxReq.newBuilder().setTime(ByteString.copyFromUtf8("0")).build();
			connection.send(MSG_StartTXReq, startTxReq);
			//System.out.println("Start tx");
			
			FpbStartTxResp startTxResp = FpbStartTxResp.parseFrom(connection.receive(MSG_StartTXResp));		
			TxId txId = startTxResp.getTxid();
			//System.out.println("Start tx: got reply"+txId.toString());
			
			
			FpbReadReq readReq = FpbReadReq.newBuilder().setTxid(txId)
					.setKey(ByteString.copyFromUtf8(key)).build();
			connection.send(MSG_ReadReq, readReq);
			//System.out.println("Read:"+key);
			
			FpbReadResp readResp = FpbReadResp.parseFrom(connection.receive(MSG_ReadResp));	
			ByteString results = readResp.getResult();
			//System.out.println("Read: got reply,"+results);
			
			FpbCommitTxReq commitTxReq = FpbCommitTxReq.newBuilder().setTxid(txId).setTag(5).build();
			connection.send(MSG_CommitTxReq, commitTxReq);
			//System.out.println("CommitTx");
			
			FpbCommitTxResp commitTxResp = FpbCommitTxResp.parseFrom(connection.receive(MSG_CommitTxResp));
			//System.out.println("CommitTx: got reply");

			//System.out.println(results);
			
			return 0;
        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
        
		return 0;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		try {
			FpbStartTxReq startTxReq = FpbStartTxReq.newBuilder().setTime(ByteString.copyFromUtf8("0")).build();
			connection.send(MSG_StartTXReq, startTxReq);
			//System.out.println("Start tx");
			
			FpbStartTxResp startTxResp = FpbStartTxResp.parseFrom(connection.receive(MSG_StartTXResp));		
			TxId txId = startTxResp.getTxid();
			//System.out.println("Start tx: got reply"+txId.toString());
			
			ByteString value = ByteString.copyFromUtf8(values.values().iterator().toString());
			FpbUpdateReq updateReq = FpbUpdateReq.newBuilder().setTxid(txId)
					.setKey(ByteString.copyFromUtf8(key)).setValue(value).build();
			connection.send(MSG_UpdateReq, updateReq);
			//System.out.println("Read:"+key);
			
			FpbUpdateResp updateResp = FpbUpdateResp.parseFrom(connection.receive(MSG_UpdateResp));	
			ByteString results = updateResp.getResult();
			//System.out.println("Read: got reply,"+results);
			
			FpbCommitTxReq commitTxReq = FpbCommitTxReq.newBuilder().setTxid(txId).setTag(5).build();
			connection.send(MSG_CommitTxReq, commitTxReq);
			//System.out.println("CommitTx");
			
			FpbCommitTxResp commitTxResp = FpbCommitTxResp.parseFrom(connection.receive(MSG_CommitTxResp));
			//System.out.println("CommitTx: got reply");

			//System.out.println(results);
			
			return 0;
            
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return 0;
	}

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		try {
			
			FpbStartTxReq startTxReq = FpbStartTxReq.newBuilder().setTime(ByteString.copyFromUtf8("0")).build();
			connection.send(MSG_StartTXReq, startTxReq);
			//System.out.println("Start tx");
			
			FpbStartTxResp startTxResp = FpbStartTxResp.parseFrom(connection.receive(MSG_StartTXResp));		
			TxId txId = startTxResp.getTxid();
			//System.out.println("Start tx: got reply"+txId.toString());
			
			//System.out.println("BeforeRead:"+key+values);
			ByteString value = ByteString.copyFromUtf8(values.values().iterator().toString());
			FpbUpdateReq readReq = FpbUpdateReq.newBuilder().setTxid(txId)
					.setKey(ByteString.copyFromUtf8(key)).setValue(value).build();
			connection.send(MSG_UpdateReq, readReq);
			//System.out.println("Read:"+key);	
			
			FpbUpdateResp readResp = FpbUpdateResp.parseFrom(connection.receive(MSG_UpdateResp));	
			ByteString results = readResp.getResult();
			//System.out.println("Read: got reply,"+results);
			
			FpbCommitTxReq commitTxReq = FpbCommitTxReq.newBuilder().setTxid(txId).setTag(5).build();
			connection.send(MSG_CommitTxReq, commitTxReq);
			//System.out.println("CommitTx");
			
			FpbCommitTxResp commitTxResp = FpbCommitTxResp.parseFrom(connection.receive(MSG_CommitTxResp));
			//System.out.println("CommitTx: got reply");
			System.out.println(results);
			
			return 0;
            
            
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return 0;
	}

	@Override
	public int delete(String table, String key) {
		// TODO Auto-generated method stub
		return 0;
	}

}
