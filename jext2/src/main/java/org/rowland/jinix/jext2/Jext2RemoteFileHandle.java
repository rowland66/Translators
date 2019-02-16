package org.rowland.jinix.jext2;

import org.rowland.jinix.io.BaseRemoteFileHandleImpl;
import org.rowland.jinix.naming.DirectoryFileData;
import org.rowland.jinix.naming.FileNameSpace;
import org.rowland.jinix.naming.RemoteFileAccessor;
import org.rowland.jinix.naming.RemoteFileHandle;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.rmi.RemoteException;
import java.util.Set;

public class Jext2RemoteFileHandle extends BaseRemoteFileHandleImpl {
    private long ino;

    Jext2RemoteFileHandle(FileNameSpace parent, String path, long inode) {
        super(parent, path);
        ino = inode;
    }

    @Override
    public RemoteFileAccessor open(int pid, Set<? extends OpenOption> set) throws FileAlreadyExistsException, NoSuchFileException {
        try {
            return ((Jext2Translator) this.getParent()).getRemoteFileAccessor(pid, this, set);
        } catch (RemoteException e) {
            throw new RuntimeException("Failure opening file: "+getPath(), e);
        }
    }

    @Override
    public DirectoryFileData getAttributes() throws NoSuchFileException {
        return super.getAttributes();
    }

    public long getInodeOffset() {
        return ino;
    }
}
