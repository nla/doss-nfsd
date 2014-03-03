package doss.nfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class BlobStoreVFS implements VirtualFileSystem {

	private static final FileHandle rootFileHandle = new FileHandle.FileHandleBuilder().build("/".getBytes()); 
	private static final Inode rootInode = new Inode(rootFileHandle);
	
	@Override
	public int access(Inode arg0, int arg1) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public nfsace4[] getAcl(Inode arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FsStat getFsStat() throws IOException {
		// TODO Auto-generated method stub
		return new FsStat(100000, 0, 0, 0);
	}

	@Override
	public Inode getRootInode() throws IOException {
		return rootInode;
	}

	@Override
	public Stat getattr(Inode inode) throws IOException {
		System.out.println("getattr called " + inode);
		// TODO Auto-generated method stub
		Stat stat = new Stat();
		stat.setMode(0755 | UnixPermission.S_IFDIR);
		return stat;
	}

	@Override
	public boolean hasIOLayout(Inode inode) throws IOException {
		// TODO what is this?
		return false;
	}

	@Override
	public List<DirectoryEntry> list(Inode inode) throws IOException {
		return new ArrayList<>(); // always empty
	}

	@Override
	public Inode lookup(Inode arg0, String path) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("lookup " + path);
		return null;
	}

	@Override
	public Inode parentOf(Inode inode) throws IOException {
		if (inode == rootInode) {
			throw new ChimeraNFSException(nfsstat.NFSERR_NOENT, "no parent");
		}
		return rootInode;
	}

	@Override
	public int read(Inode arg0, byte[] arg1, long arg2, int arg3)
			throws IOException {
		// TODO
		return 0;
	}
	
	//
	// Unsupported operations
	//
	
	@Override
	public String readlink(Inode arg0) throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "not a symbolic link");		
	}
	
	@Override
	public Inode mkdir(Inode arg0, String arg1, int arg2, int arg3, int arg4)
			throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}

	@Override
	public void move(Inode arg0, String arg1, Inode arg2, String arg3)
			throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}
	
	@Override
	public Inode link(Inode arg0, Inode arg1, String arg2, int arg3, int arg4)
			throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}
	
	@Override
	public Inode create(Inode arg0, Type arg1, String arg2, int arg3, int arg4,
			int arg5) throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}

	@Override
	public void remove(Inode arg0, String arg1) throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}

	@Override
	public void setAcl(Inode arg0, nfsace4[] arg1) throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}

	@Override
	public void setattr(Inode arg0, Stat arg1) throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}

	@Override
	public Inode symlink(Inode arg0, String arg1, String arg2, int arg3,
			int arg4, int arg5) throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}

	@Override
	public int write(Inode arg0, byte[] arg1, long arg2, int arg3)
			throws IOException {
		throw new ChimeraNFSException(nfsstat.NFSERR_ROFS, "doss-nfsd does not support writing");
	}
}
