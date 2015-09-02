package doss.nfs;


import org.dcache.nfs.ChimeraNFSException;


    public class DossNFSException extends ChimeraNFSException {

        public static final long serialVersionUID = 99621134L;
        public DossNFSException(int status, String msg) {
            super(status,msg);
        }
    }

