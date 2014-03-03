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
		URL exports = Main.class.getClassLoader().getResource(
				"doss/nfs/exports");
		if (exports == null) {
			throw new RuntimeException(
					"doss/nfs/exports not found on classpath");
		}
		VirtualFileSystem fs = new BlobStoreVFS();
		ExportFile exportFile = new ExportFile(exports);
		
		// NFS 3
		NfsServerV3 nfs3 = new NfsServerV3(exportFile, fs);
		
		// NFS 4
		MDSOperationFactory opfac = new MDSOperationFactory();
		NfsIdMapping idMap = new SimpleIdMap();
		
		NFSServerV41 nfs4 = new NFSServerV41(opfac, null, fs, idMap, exportFile);
		
		MountServer mountd = new MountServer(exportFile, fs);

		OncRpcSvc rpcSvc = new OncRpcSvcBuilder().withPort(1234).withTCP()
				.build();
		Map<OncRpcProgram, RpcDispatchable> services = new HashMap<>();
		
		rpcSvc.register(new OncRpcProgram(100003, 3), nfs3);
		rpcSvc.register(new OncRpcProgram(100003, 4), nfs4);
		rpcSvc.register(new OncRpcProgram(100005, 3), mountd);
		
		rpcSvc.setPrograms(services );
		rpcSvc.start();
		Thread.sleep(100000);
	}
}
