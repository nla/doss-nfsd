package doss.nfs;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.OncRpcException;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.dcache.xdr.RpcDispatchable;
import org.dcache.xdr.portmap.OncRpcEmbeddedPortmap;

public class Main {
	public static void main(String[] args) throws IOException, OncRpcException, InterruptedException {
		// virtual file system
		VirtualFileSystem fs = new BlobStoreVFS();
		
		// exports config file
		URL exportsUrl = Main.class.getClassLoader().getResource("doss/nfs/exports");
		if (exportsUrl == null) {
			throw new RuntimeException("doss/nfs/exports not found on classpath");
		}
		ExportFile exports = new ExportFile(exportsUrl);
		
		// NFS 4 server
		MDSOperationFactory opfac = new MDSOperationFactory();
		NfsIdMapping idMap = new SimpleIdMap();
		NFSServerV41 nfs4 = new NFSServerV41(opfac, null, fs, idMap, exports);
		
		// mountd
		MountServer mountd = new MountServer(exports, fs);

		// RPC server
		OncRpcSvc rpcSvc = new OncRpcSvcBuilder().withPort(1234).withTCP().build();
		Map<OncRpcProgram, RpcDispatchable> services = new HashMap<>();
		rpcSvc.register(new OncRpcProgram(100003, 4), nfs4);
		rpcSvc.register(new OncRpcProgram(100005, 3), mountd);
		rpcSvc.setPrograms(services );
		rpcSvc.start();
		
		try {
			// hang around for a while
			Thread.sleep(100000);
		} finally {
			rpcSvc.stop();
		}
	}
}
