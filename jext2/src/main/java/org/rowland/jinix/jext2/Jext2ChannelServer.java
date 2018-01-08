package org.rowland.jinix.jext2;

import jext2.*;
import jext2.exceptions.IoError;
import jext2.exceptions.JExt2Exception;
import jext2.exceptions.NoSuchFileOrDirectory;
import org.rowland.jinix.JinixKernelUnicastRemoteObject;
import org.rowland.jinix.naming.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Jext2Translator server for files.
 */
public class Jext2ChannelServer extends JinixKernelUnicastRemoteObject implements RemoteFileAccessor, FileAccessorStatistics {

    private Jext2Translator translator;
    private long ino;
    private long dirIno;
    private int pid;
    private String name;
    private long position;
    private RegularInode inode;
    private boolean closed;
    private ReentrantLock positionOperationLock = new ReentrantLock(false);
    private int openCount;

    Jext2ChannelServer(Jext2Translator translator, int pid, String name, long parentIno, long ino, Set<? extends OpenOption> options)
            throws NoSuchFileException, FileAlreadyExistsException, RemoteException {
        try {
            this.translator = translator;
            this.pid = pid;
            this.name = name;
            this.dirIno = parentIno;
            this.ino = ino;
            closed = false;
            openCount = 1;

            if (options == null) {
                options = Collections.emptySet();
            }

            if (ino == -1) {
                if (!(options.contains(StandardOpenOption.CREATE) ||
                        options.contains(StandardOpenOption.CREATE_NEW))) {
                    throw new NoSuchFileException(name);
                }
                DirectoryInode dirInode = (DirectoryInode) translator.inodes.openInode(dirIno);
                try {
                    inode = RegularInode.createEmpty();
                    inode.setMode(new ModeBuilder()
                            .regularFile()
                            .create());
                    InodeAlloc.registerInode(dirInode, inode);
                    inode.write();
                    dirInode.addLink(inode, name);
                } finally {
                    translator.inodes.forgetInode(dirIno, 1);
                }
            } else {
                if (options.contains(StandardOpenOption.CREATE_NEW)) {
                    throw new FileAlreadyExistsException(name);
                }
                Inode in = translator.inodes.openInode(ino);
                if (!(in instanceof RegularInode)) {
                    throw new RemoteException("illegal attempt to open non-file");
                } else {
                    inode = (RegularInode) in;
                }
            }

        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error opening Jext2ChannelServer", e);
        }
    }

    @Override
    public byte[] read(int pgid, int len) throws RemoteException {
        if (openCount == 0) {
            throw new RemoteException("Illegal attempt to read from a closed file descriptor");
        }
        this.positionOperationLock.lock();
        try {
            ByteBuffer buf = inode.readData(len, this.getPosition());
            if (buf == null) { // EOF
                return null;
            }
            byte[] rtrn = new byte[buf.limit()];
            buf.get(rtrn);
            this.setPosition(this.getPosition() + rtrn.length);
            return rtrn;
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2Exception on read()", e);
        } finally {
            this.positionOperationLock.unlock();
        }
    }

    @Override
    public int write(int pgid, byte[] bytes) throws RemoteException {

        if (openCount == 0) {
            throw new RemoteException("Illegal attempt to write to a closed file descriptor");
        }

        this.positionOperationLock.lock();
        try {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int bw = inode.writeData(bb, this.getPosition());
            this.setPosition(this.getPosition() + bw);
            inode.setModificationTime(new Date());
            inode.sync();
            return bw;
        } catch (JExt2Exception e) {
            throw new RemoteException("failure during write", e);
        } finally {
            this.positionOperationLock.unlock();
        }
    }

    @Override
    public long skip(long l) throws RemoteException {
        this.positionOperationLock.lock();
        try {
            this.setPosition(this.getPosition() + l);
            return l;
        } finally {
            this.positionOperationLock.unlock();
        }
    }

    @Override
    public int available() throws RemoteException {
        return (int) Math.min((long) Integer.MAX_VALUE, inode.getSize() - this.getPosition());
    }

    @Override
    public long getFilePointer() throws RemoteException {
        return this.getPosition();
    }

    @Override
    public void seek(long l) throws RemoteException {
        this.setPosition(l);
    }

    @Override
    public long length() throws RemoteException {
        return this.inode.getSize();
    }

    @Override
    public void setLength(long l) throws RemoteException {
        this.inode.setSize(l);
    }

    @Override
    public void duplicate() throws RemoteException {
        openCount++;
    }

    @Override
    public void close() throws RemoteException {
        try {
            if (openCount > 0) {
                openCount--;
                if (openCount == 0) {
                    inode.sync();
                    translator.inodes.forgetInode(this.inode.getIno(), 1);
                    if (!unexport()) {
                        Filesystem.getLogger().severe("FSCS unexport failed: "+this.toString());
                    }
                }
            }
        } catch (IoError ioError) {
            throw new RemoteException("IoError closing FileInputStream", ioError);
        } finally {
            if (openCount == 0) {
                translator.removeFileSystemChannelServer(pid, this);
            }
        }
    }

    long getPosition() {
        return position;
    }

    void setPosition(long position) {
        this.position = position;
    }

    @Override
    public void force(boolean b) throws RemoteException {
        // TODO: implement force
    }

    public String getAbsolutePathName() throws RemoteException {
        return name;
    }

}
