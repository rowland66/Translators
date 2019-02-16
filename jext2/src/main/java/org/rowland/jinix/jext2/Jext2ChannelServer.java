package org.rowland.jinix.jext2;

import jext2.*;
import jext2.exceptions.*;
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
import java.util.logging.Level;

/**
 * Jext2Translator server for files.
 */
public class Jext2ChannelServer extends JinixKernelUnicastRemoteObject implements RemoteFileAccessor, FileAccessorStatistics {

    private Jext2Translator translator;
    private long ino;
    private int pid;
    private String name;
    private long position;
    private RegularInode inode;
    private boolean closed;
    private ReentrantLock positionOperationLock = new ReentrantLock(false);
    private int openCount;

    Jext2ChannelServer(Jext2Translator translator, int pid, String pathName, long ino, Set<? extends OpenOption> options)
            throws NoSuchFileException, RemoteException {

        try {
            this.translator = translator;
            this.pid = pid;
            this.name = pathName;
            this.ino = ino;
            closed = false;
            openCount = 1;

            Inode in = translator.inodes.openInode(ino);
            if (!(in instanceof RegularInode)) {
                translator.inodes.forgetInode(ino, 1);
                throw new NoSuchFileException("illegal attempt to open non-file");
            } else {
                inode = (RegularInode) in;
            }

        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error opening Jext2ChannelServer", e);
        }
    }

    @Override
    public RemoteFileHandle getRemoteFileHandle() throws RemoteException {
        return new Jext2RemoteFileHandle(translator, name, ino);
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
            int bw = inode.writeData(bytes, this.getPosition());
            this.setPosition(this.getPosition() + bw);
            inode.setModificationTime(new Date());
            inode.sync();
            return bw;
        } catch (NoSpaceLeftOnDevice e) {
            throw new RemoteException("Not enough space");
        } catch (FileTooLarge e) {
            throw new RemoteException("File too large");
        } catch (JExt2Exception e) {
            System.err.println("failure during write: "+e.getMessage());
            e.printStackTrace(System.err);
            System.err.flush();
            throw new RuntimeException("failure during write: "+e.getMessage());
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
