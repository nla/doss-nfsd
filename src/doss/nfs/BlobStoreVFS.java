package doss.nfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.dcache.chimera.UnixPermission;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FileHandle;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.status.*;

import doss.Blob;
import doss.BlobStore;
import doss.NoSuchBlobException;

import static org.dcache.nfs.v4.xdr.nfs4_prot.*;

public class BlobStoreVFS implements VirtualFileSystem {

	private static final String BLOB_PREFIX = "nla.doss-";
	private static final FileHandle rootFileHandle = new FileHandle.FileHandleBuilder()
			.build("/".getBytes());
	private static final Inode rootInode = new Inode(rootFileHandle);
	private static final byte[] README_TEXT = "This filesystem is a NFS to DOSS gateway.\nAccess invisible files like /123.anyextension to read DOSS blobs.\n".getBytes();

	private final BlobStore blobStore;

	public BlobStoreVFS(BlobStore blobStore) {
		this.blobStore = blobStore;
	}

        @Override
        public NfsIdMapping getIdMapper() {
            return(new SimpleIdMap());
        }

        @Override
        public AclCheckable getAclCheckable() {
            return(AclCheckable.ALLOW_ALL);
        }

	@Override
	public int access(Inode inode, int mode) throws IOException {
		return mode & (ACCESS4_READ | ACCESS4_LOOKUP); // everything is
														// read-only
	}

	@Override
	public FsStat getFsStat() throws IOException {
		return new FsStat(10L * 1024 * 1024 * 1024, 0, 0, 0);
	}

	@Override
	public Inode getRootInode() throws IOException {
		return rootInode;
	}

	@Override
	public Stat getattr(Inode inode) throws IOException {
		String path = new String(inode.getFileId());
		Stat stat = new Stat();
		stat.setGid(0);
		stat.setUid(0);
		stat.setNlink(1);
		if ("/".equals(path)) {
			stat.setMode(0555 | UnixPermission.S_IFDIR);
                        stat.setGeneration(0);
			stat.setSize(0);
			long now = System.currentTimeMillis();
			stat.setATime(now);
			stat.setMTime(now);
			stat.setCTime(now);
			stat.setIno(0);
			stat.setFileid(0);
		} else if ("/README".equals(path)) {
			stat.setMode(0444 | UnixPermission.S_IFREG);
                        stat.setGeneration(0);
			stat.setSize(README_TEXT.length);
			stat.setATime(1393987443000L);
			stat.setMTime(1393987443000L);
			stat.setCTime(1393987443000L);
			stat.setIno(-1);
			stat.setFileid(-1);			
		} else {
			stat.setMode(0444 | UnixPermission.S_IFREG);
                        stat.setGeneration(0);
			Blob blob = inodeToBlob(inode);
			if (blob == null) {
				throw new NoEntException("no such blob");
			}
			stat.setSize(blob.size());
			stat.setMTime(blob.created().toMillis());
			stat.setCTime(blob.created().toMillis());
			stat.setATime(blob.created().toMillis());
			stat.setIno((int)blob.id());
			stat.setFileid(blob.id());
		}		
		return stat;
	}

	@Override
	public List<DirectoryEntry> list(Inode inode) throws IOException {
		List<DirectoryEntry> list = new ArrayList<>();
		list.add(new DirectoryEntry("README", toInode("/README"),
				getattr(toInode("/README"))));
		return list;
	}

	@Override
	public Inode lookup(Inode parentInode, String path) throws IOException {
		if (!"/".equals(inodePath(parentInode))) {
			System.out.println("bad blobid2 " + path + " .. " + parentInode
					+ " vs " + rootInode);
			// we don't support directories except for the root
			throw new NoEntException("not found");
		}
		if (path.equals("README")) {
			return toInode("/README");
		}
		Long blobId = pathToBlobId(path);
		if (blobId == null) {
			throw new NoEntException("bad blobId");
		}
		try {
			blobStore.get(blobId);
		} catch (NoSuchBlobException e) {
			throw new NoEntException("no such blob");
		}
		return toInode(BLOB_PREFIX + blobId);
	}

	private Long pathToBlobId(String path) throws IOException {
		int dotPos = path.indexOf(".");
		String id;
		if (dotPos >= 0) {
			id = path.substring(0, dotPos);
		} else {
			id = path;
		}
		try {
			return Long.parseLong(id);
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private Inode toInode(String path) {
		return new Inode(new FileHandle.FileHandleBuilder().build(path
				.getBytes()));
	}

	private String inodePath(Inode inode) {
		return new String(inode.getFileId());
	}

	private Blob inodeToBlob(Inode inode) throws IOException {
		String path = inodePath(inode);
		if (path.startsWith(BLOB_PREFIX)) {
			try {
				return blobStore.get(Long.parseLong(path.substring(BLOB_PREFIX
						.length())));
			} catch (NoSuchBlobException | NumberFormatException e) {
				return null;
			}
		} else {
			return null;
		}
	}

	@Override
	public Inode parentOf(Inode inode) throws IOException {
		if (inode == rootInode) {
			throw new NoEntException("no parent");
		}
		return rootInode;
	}

	@Override
	public int read(Inode inode, byte[] data, long offset, int count)
			throws IOException {	
		Blob blob = inodeToBlob(inode);
		if (blob != null) {
			return readFromBlob(blob, data, offset, count);
		} else {
			return readFromReadme(data, offset, count);
		}
	}

	private int readFromBlob(Blob blob, byte[] data, long offset, int count)
			throws IOException {
		try (SeekableByteChannel ch = blob.openChannel()) {
			ByteBuffer buf = ByteBuffer.wrap(data, 0, count);
			ch.position(offset);
			ch.read(buf);
			return buf.position();
		}
	}

	private int readFromReadme(byte[] data, long offset, int count) {
		int nbytes = (int)Math.min(count - offset, README_TEXT.length);
		System.arraycopy(README_TEXT, (int)offset, data, 0, nbytes);
		return nbytes;
	}

	//
	// Unsupported operations
	//

	@Override
	public boolean hasIOLayout(Inode inode) throws IOException {
		return false; // all data is local to the server
	}

	@Override
	public nfsace4[] getAcl(Inode inode) throws IOException {
		return new nfsace4[0];
	}

	@Override
	public String readlink(Inode arg0) throws IOException {
		throw new InvalException("not a symbolic link");
	}

	@Override
        public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
	public boolean move(Inode arg0, String arg1, Inode arg2, String arg3) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
        public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
        public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
	public void remove(Inode arg0, String arg1) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
	public void setAcl(Inode arg0, nfsace4[] arg1) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
	public void setattr(Inode arg0, Stat arg1) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
        public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}

	@Override
        public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
	}
        @Override
        public void commit(Inode inode, long offset, int count) throws IOException {
                throw new RoFsException("doss-nfsd does not support writing");
        }
}
