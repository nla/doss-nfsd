package doss.nfs;

import doss.BlobStore;
import doss.local.LocalBlobStore;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.ExportFile;
import org.dcache.nfs.status.BadSeqidException;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.xdr.mount_prot;
import org.dcache.nfs.v4.*;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.seqid4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.*;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

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
			NFSServerV41 nfs4 = new NFSServerV41(opfac, devManager, fs,	exports);
			monkeyPatchSeqidValidation(nfs4);

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

	/**
	 * nfs4j upstream keep breaking seqid validation.  Most recently (at time of writing) in:
	 *
	 * https://github.com/dCache/nfs4j/commit/e98a227171fff6cd37cc6f4704ede4f9e539bdb5
	 *
	 * As far as I can tell seqids should be associated with an NFS state not with the client. At least that's what
	 * Linux and Solaris NFS 4.0 clients are assuming.  The problem is easy to reproduce.  Mount on a Linux or Solaris
	 * box and try to access a file as two differnt users, the first will work, the second will hang and eventually
	 * error.
	 *
	 * For our use case seqid validation is not important so lets monkey patch it out for now until nfs4j gets fixed.
	 *
	 */
	static void monkeyPatchSeqidValidation(NFSServerV41 server) {
		try {
			Field f = NFSServerV41.class.getDeclaredField("_statHandler");
			f.setAccessible(true);
			f.set(server, new PatchedStateHandler());
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("monkey patching failed", e);
		}
	}

	static class PatchedStateHandler extends NFSv4StateHandler {
		static MethodHandle addClientMethod, nextClientIdMethod, leaseTimeGetter;

		static {
			try {
				Method m = NFSv4StateHandler.class.getDeclaredMethod("addClient", NFS4Client.class);
				m.setAccessible(true);
				addClientMethod = MethodHandles.lookup().unreflect(m);

				m = NFSv4StateHandler.class.getDeclaredMethod("nextClientId");
				m.setAccessible(true);
				nextClientIdMethod = MethodHandles.lookup().unreflect(m);

				Field f = NFSv4StateHandler.class.getDeclaredField("_leaseTime");
				f.setAccessible(true);
				leaseTimeGetter = MethodHandles.lookup().unreflectGetter(f);

			} catch (NoSuchMethodException | NoSuchFieldException | IllegalAccessException e) {
				throw new RuntimeException("monkey patching failed", e);
			}
		}

		@Override
		public NFS4Client createClient(InetSocketAddress clientAddress, InetSocketAddress localAddress, int minorVersion, byte[] ownerID, verifier4 verifier, Principal principal, boolean callbackNeeded) {
			try {
				NFS4Client client = new PatchedClient((Long)nextClientIdMethod.invoke(this), minorVersion, clientAddress, localAddress, ownerID, verifier, principal, (Long)leaseTimeGetter.invoke(this), callbackNeeded);
				addClientMethod.invoke(this, client);
				return client;
			} catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}

		}
	}

	static class PatchedClient extends NFS4Client {
		static MethodHandle clientStatesGetter;

		static {
			try {
				Field f = NFS4Client.class.getDeclaredField("_clientStates");
				f.setAccessible(true);
				clientStatesGetter = MethodHandles.lookup().unreflectGetter(f);
			} catch (IllegalAccessException | NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
		}

		public PatchedClient(long clientId, int minorVersion, InetSocketAddress clientAddress, InetSocketAddress localAddress, byte[] ownerID, verifier4 verifier, Principal principal, long leaseTime, boolean calbackNeeded) {
			super(clientId, minorVersion, clientAddress, localAddress, ownerID, verifier, principal, leaseTime, calbackNeeded);
		}

		@Override
		public NFS4State createState() throws ChimeraNFSException {
			/*
			 * The _clientState table slowly leaks over time as Linux clients sometimes send an OPEN without a
			 * corresponding CLOSE.  In particular we've seen this when the process killed during the open() syscall.
			 * As (another) dirty hack to keep things from breaking let's clear out the _clientStates just before it
			 * fills up.
			 */
			try {
				Map<stateid4, NFS4State> clientStates = (Map<stateid4, NFS4State>) clientStatesGetter.invoke(this);
				if (clientStates.size() > 16000) {
					for (stateid4 key : clientStates.keySet()) {
						NFS4State state = clientStates.remove(key);
						state.tryDispose();
					}
				}
			} catch (Throwable throwable) {
				throw new RuntimeException(throwable);
			}

			return super.createState();
		}

		@Override
		public synchronized void validateSequence(seqid4 openSeqid) throws BadSeqidException {
			// disabled
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
