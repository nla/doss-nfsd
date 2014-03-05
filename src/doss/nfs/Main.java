package doss.nfs;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v4.DeviceManager;
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

import doss.BlobStore;
import doss.local.LocalBlobStore;

public class Main {
	public static void main(String[] args) throws IOException, OncRpcException,
			InterruptedException {
		if (args.length < 1) {
			System.err.println("Usage: doss-nfsd blobstore-path");
			System.exit(1);
		}

		try (BlobStore blobStore = LocalBlobStore.open(Paths.get(args[0]))) {

			// virtual file system
			VirtualFileSystem fs = new BlobStoreVFS(blobStore);

			// exports config file
			URL exportsUrl = Main.class.getClassLoader().getResource(
					"doss/nfs/exports");
			if (exportsUrl == null) {
				throw new RuntimeException(
						"doss/nfs/exports not found on classpath");
			}
			ExportFile exports = new ExportFile(exportsUrl);

			// NFS 4 server
			DeviceManager devManager = new DeviceManager();
			MDSOperationFactory opfac = new MDSOperationFactory();
			NfsIdMapping idMap = new SimpleIdMap();
			NFSServerV41 nfs4 = new NFSServerV41(opfac, devManager, fs, idMap,
					exports);

			// mountd
			MountServer mountd = new MountServer(exports, fs);

			// RPC server
			OncRpcSvc rpcSvc = new OncRpcSvcBuilder().withPort(1234).withTCP()
					.build();
			Map<OncRpcProgram, RpcDispatchable> services = new HashMap<>();
			rpcSvc.register(new OncRpcProgram(100003, 4), nfs4);
			rpcSvc.register(new OncRpcProgram(100005, 3), mountd);
			rpcSvc.setPrograms(services);
			rpcSvc.start();

			try {
				// hang around for a while
				while (true) {
					Thread.sleep(100000000);
				}
			} finally {
				rpcSvc.stop();
			}
		}
	}
}
