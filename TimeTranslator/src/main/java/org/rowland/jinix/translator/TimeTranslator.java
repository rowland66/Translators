package org.rowland.jinix.translator;

import org.rowland.jinix.JinixKernelUnicastRemoteObject;
import org.rowland.jinix.io.JinixFile;
import org.rowland.jinix.io.JinixFileDescriptor;
import org.rowland.jinix.lang.JinixRuntime;
import org.rowland.jinix.lang.ProcessSignalHandler;
import org.rowland.jinix.naming.*;
import org.rowland.jinix.nio.JinixFileChannel;
import org.rowland.jinix.proc.ProcessManager;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.Remote;
import java.rmi.RemoteException;

import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * A simple translator that translates a single file an returns the current time
 */
public class TimeTranslator extends JinixKernelUnicastRemoteObject implements FileNameSpace, TimeKeeper {
    private static TimeTranslator translator;
    private JinixFileChannel translatorNode;
    private static Thread mainThread;

    private int offset; // Adjustment to the time in milli-seconds.

    private TimeTranslator(JinixFileChannel translatorNode) throws RemoteException {
        super();
        this.translatorNode = translatorNode;
    }


    @Override
    public DirectoryFileData getFileAttributes(String fileName) throws NoSuchFileException, RemoteException {
        DirectoryFileData rtrn = new DirectoryFileData();
        rtrn.type = DirectoryFileData.FileType.FILE;
        rtrn.lastModified = System.currentTimeMillis();
        rtrn.length = 0;
        rtrn.name = "Unknown";
        return rtrn;
    }

    @Override
    public URI getURI() throws RemoteException {
        return null;
    }

    @Override
    public void setFileAttributes(String fileName, DirectoryFileData directoryFileData) throws NoSuchFileException, RemoteException {

    }

    @Override
    public String[] list(String directory) throws RemoteException {
        return null;
    }

    @Override
    public boolean createFileAtomically(String directory, String name) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public boolean createDirectory(String directory, String name) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public void delete(String file) throws NoSuchFileException, DirectoryNotEmptyException, RemoteException {

    }

    @Override
    public void copy(RemoteFileHandle sourceFile, RemoteFileHandle targetDirectory, String fileName, CopyOption... copyOptions) {

    }

    @Override
    public void move(RemoteFileHandle sourceFile, RemoteFileHandle targetDirectory, String fileName, CopyOption... copyOptions) {

    }

    @Override
    public Object lookup(int pid, String filePath) throws RemoteException {
        return null;
    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, String name, Set<? extends OpenOption> openOptions) throws RemoteException {
        String timeStr = String.format("%1$tb %1$td %1$tY  %1$tH:%1$tM:%1$tS"+"\n",System.currentTimeMillis());
        return new TimeTranslatorFileChannel(timeStr);
    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, RemoteFileHandle remoteFileHandle, Set<? extends OpenOption> openOptions) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {
        return getRemoteFileAccessor(pid, remoteFileHandle.getPath(), openOptions);
    }

    @Override
    public FileNameSpace getParent() throws RemoteException {
        return null;
    }

    @Override
    public String getPathWithinParent() throws RemoteException {
        return null;
    }

    @Override
    public List<FileAccessorStatistics> getOpenFiles(int i) throws RemoteException {
        return Collections.emptyList();
    }

    @Override
    public void setTimeOffset(int offset) throws RemoteException {
        this.offset = offset;
    }

    @Override
    public TimeWithOffset getTimeWithOffset() throws RemoteException {
        return new TimeWithOffsetImpl(System.currentTimeMillis(), this.offset);
    }

    public static class TimeTranslatorFileChannel extends JinixKernelUnicastRemoteObject implements RemoteFileAccessor {

        private ByteArrayInputStream d;
        private int openCount;

        TimeTranslatorFileChannel(String data) throws RemoteException {
            super();
            d = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
            openCount = 1;
        }

        @Override
        public RemoteFileHandle getRemoteFileHandle() throws RemoteException {
            return null;
        }

        @Override
        public byte[] read(int pgid, int len) throws RemoteException {
            if (d.available() == 0) {
                return null;
            }
            try {
                byte[] rtrn = new byte[Math.min(d.available(),len)];
                d.read(rtrn);
                return rtrn;
            } catch (IOException e) {
                throw new RemoteException("Internal error", e);
            }
        }

        @Override
        public int write(int pgid, byte[] bytes) throws RemoteException {
            return bytes.length;
        }

        @Override
        public long skip(long l) throws RemoteException {
            if (d == null) {
                return 0;
            }
            return d.skip(l);
        }

        @Override
        public int available() throws RemoteException {
            if (d == null) {
                return 0;
            }
            return d.available();
        }

        @Override
        public long getFilePointer() throws RemoteException {
            return 0;
        }

        @Override
        public void seek(long l) throws RemoteException {

        }

        @Override
        public long length() throws RemoteException {
            return 0;
        }

        @Override
        public void setLength(long l) throws RemoteException {

        }

        @Override
        public void duplicate() throws RemoteException {
            openCount++;
        }

        @Override
        public void force(boolean b) throws RemoteException {

        }

        @Override
        public void close() throws RemoteException {
            if (openCount > 0) {
                openCount--;
                if (openCount == 0) {
                    d = null;
                    unexport();
                }
            }
        }
    }

    public static void main(String[] args) {

        JinixFile jinixFile = JinixRuntime.getRuntime().getTranslatorFile();
        if (jinixFile == null) {
            System.err.println("Translator must be started with settrans");
            return;
        }
        try {
            translator = new TimeTranslator(null);
        } catch (IOException e) {
            throw new RuntimeException("Translator failed initialization",e);
        }

        JinixRuntime.getRuntime().bindTranslator(translator);

        mainThread = Thread.currentThread();

        JinixRuntime.getRuntime().registerSignalHandler(new ProcessSignalHandler() {
            @Override
            public boolean handleSignal(ProcessManager.Signal signal) {
                if (signal == ProcessManager.Signal.TERMINATE) {
                    mainThread.interrupt();
                    return true;
                }
                return false;
            }
        });

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            // Interrupted shutting down
        }

        translator.unexport();
    }

    public static class TimeWithOffsetImpl implements TimeWithOffset, Serializable {
        private long time;
        private int offset;

        private TimeWithOffsetImpl(long time, int offset) {
            this.time = time;
            this.offset = offset;
        }
        @Override
        public long getTime() {
            return time;
        }

        @Override
        public int getOffset() {
            return offset;
        }
    }
}
