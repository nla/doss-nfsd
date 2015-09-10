package doss.nfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v3.xdr.nfs3_prot;
import org.dcache.nfs.v4.xdr.nfs4_prot;
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

import sun.misc.*;

public class Main {
	public static void main(String[] args) throws IOException, OncRpcException,
			InterruptedException {
		if (args.length < 3) {
			System.err.println("Usage: port doss-nfsd blobstore-path exports-file");
			System.err.println("");
			System.err.println("The exports file is the same format as /etc/exports.  Use / as the path.");
			System.err.println("You can send SIGHUP to reload it without restarting.");
			System.exit(1);
		}
		
		int port = Integer.parseInt(args[0]);
		String blobStorePath = args[1];
		String exportsFilePath = args[2];

		try (BlobStore blobStore = LocalBlobStore.open(Paths.get(blobStorePath))) {

			// virtual file system
			VirtualFileSystem fs = new BlobStoreVFS(blobStore);

			// exports config file			
			ExportFile exports = new ExportFile(new File(exportsFilePath));
			registerSignalHandler(exports);

			// NFS 4 server
			DeviceManager devManager = new DeviceManager();
			MDSOperationFactory opfac = new MDSOperationFactory();
			NFSServerV41 nfs4 = new NFSServerV41(opfac, devManager, fs,
					exports);

			// mountd
			MountServer mountd = new MountServer(exports, fs);

			// RPC server
			OncRpcSvc rpcSvc = new OncRpcSvcBuilder().withPort(port).withTCP().withAutoPublish().withSameThreadIoStrategy()
					.build();
			Map<OncRpcProgram, RpcDispatchable> services = new HashMap<>();
			rpcSvc.register(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), nfs4);
			rpcSvc.register(new OncRpcProgram(mount_prot.MOUNT_PROGRAM, mount_prot.MOUNT_V3), mountd);
			//rpcSvc.setPrograms(services);
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

	@SuppressWarnings("restriction")
	private static void registerSignalHandler(final ExportFile exports) {
		Signal.handle(new Signal("HUP"), new SignalHandler() {
		    public void handle(Signal signal)
		    {
		        try {
		        	System.out.println("HUP signal received, rescanning exports file.");
		        	exports.rescan();
				} catch (IOException e) {
					throw new RuntimeException("Error reloading exports file", e);
				}
		    }
		});
	}
}
