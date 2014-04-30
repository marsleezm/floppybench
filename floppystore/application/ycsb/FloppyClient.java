package floppystore.application.ycsb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.ericsson.otp.erlang.*;

public class FloppyClient extends DB {
	
	private OtpSelf self = null;
	private OtpPeer peer = null;
    private OtpConnection connection = null;
    private String serverNode = "floppy@127.0.0.1";
    private String server = "floppy";
	
    
    public void init() throws DBException {
        try {
			self = new OtpSelf("node@127.0.0.1","floppy");
			peer  = new OtpPeer(serverNode); 
			connection = self.connect(peer); 

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (OtpAuthException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    
	
	@Override
	// TODO Table is actually type!
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
		OtpErlangAtom keyStr = new OtpErlangAtom(key);
		OtpErlangAtom typeAtom = new OtpErlangAtom("riak_dt_gcounter");
        OtpErlangList msg =
            new OtpErlangList(
                               new OtpErlangObject[] {
                                   keyStr,
                                   typeAtom
                               });
        
        connection.sendRPC(server,"read", msg);
		
        System.out.println("Message "+ msg.toString());
        
        OtpErlangObject received = connection.receiveRPC(); 
        
        System.out.println("Received somthing");
        result.put(key, new StringByteIterator(received.toString()));
        return 0;
        
		} catch (OtpErlangExit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (OtpAuthException e) {
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
			//System.out.println("Starting");
			OtpErlangAtom keyAtom = new OtpErlangAtom(key);
			OtpErlangObject fields[] = new OtpErlangObject[values.size()];
        
			String op = "increment",//values.get("op").toString(),
        		actor = "haha";//values.get("actor").toString();
        
			//System.out.println("Preparing message1");
			OtpErlangTuple param = 
        		new OtpErlangTuple(
                        new OtpErlangObject[] {
                            new OtpErlangAtom(op),
                            new OtpErlangAtom(actor)
                        });
			
			//System.out.println("Preparing message2");
        
			OtpErlangList msg =
                new OtpErlangList(
                                   new OtpErlangObject[] {
                                       keyAtom,
                                       param
                                   });
			
			connection.sendRPC(server,"update", msg);
            //System.out.println("Message "+ msg.toString());
            OtpErlangObject received = connection.receiveRPC(); 
            //System.out.println("Received message "+received);
            return 0;
            
			} catch (OtpErlangExit e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OtpAuthException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
			//System.out.println("Starting");
			OtpErlangAtom keyAtom = new OtpErlangAtom(key);
			OtpErlangObject fields[] = new OtpErlangObject[values.size()];
        
			String op = "increment",//values.get("op").toString(),
	        		actor = "haha";//values.get("actor").toString();
        
			//System.out.println("Preparing message1");
			OtpErlangTuple param = 
        		new OtpErlangTuple(
                        new OtpErlangObject[] {
                            new OtpErlangAtom(op),
                            new OtpErlangAtom(actor)
                        });
			
			//System.out.println("Preparing message2");
        
			OtpErlangList msg =
                new OtpErlangList(
                                   new OtpErlangObject[] {
                                       keyAtom,
                                       param
                                   });
			
			connection.sendRPC(server,"update", msg);
            //System.out.println("Message "+ msg.toString());
            OtpErlangObject received = connection.receiveRPC(); 
            //System.out.println("Received message "+received);
            return 0;
            
			} catch (OtpErlangExit e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OtpAuthException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
