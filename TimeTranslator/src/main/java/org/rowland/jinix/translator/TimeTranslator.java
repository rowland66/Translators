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
import java.rmi.RemoteException;

import java.rmi.server.UnicastRemoteObject;

/**
 * A simple translator that translates a single file an returns the current time
 */
public class TimeTranslator extends JinixKernelUnicastRemoteObject implements FileNameSpace {
    private static TimeTranslator translator;
    private JinixFileChannel translatorNode;
    private static Thread mainThread;

    private TimeTranslator(JinixFileChannel translatorNode) throws RemoteException {
        super();
        this.translatorNode = translatorNode;
    }
    @Override
    public DirectoryFileData getFileAttributes(String name) throws NoSuchFileException, RemoteException {
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
    public void setFileAttributes(String name, DirectoryFileData directoryFileData) throws NoSuchFileException, RemoteException {

    }

    @Override
    public boolean exists(String name) throws RemoteException {
        return true;
    }

    @Override
    public String[] list(String name) throws RemoteException {
        return null;
    }

    @Override
    public DirectoryFileData[] listDirectoryFileData(String name) throws RemoteException {
        return new DirectoryFileData[0];
    }

    @Override
    public boolean createFileAtomically(String name) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public boolean createDirectory(String name) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public boolean createDirectories(String name) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public void delete(String name) throws NoSuchFileException, DirectoryNotEmptyException, RemoteException {

    }

    @Override
    public void copy(String name, String s, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, RemoteException {

    }

    @Override
    public void move(String name, String s, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, RemoteException {

    }

    @Override
    public FileChannel getFileChannel(String s, OpenOption... openOptions) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {
        String timeStr = String.format("%1$tb %1$td %1$tY  %1$tH:%1$tM:%1$tS"+"\n",System.currentTimeMillis());
        return new TimeTranslatorFileChannel(timeStr);
    }

    @Override
    public Object getKey(String name) throws RemoteException {
        return name;
    }

    public static class TimeTranslatorFileChannel extends JinixKernelUnicastRemoteObject implements FileChannel {

        private ByteArrayInputStream d;
        private int openCount;

        TimeTranslatorFileChannel(String data) throws RemoteException {
            super();
            d = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
            openCount = 1;
        }

        @Override
        public byte[] read(int len) throws RemoteException {
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
        public int write(byte[] bytes) throws RemoteException {
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

        JinixFileDescriptor fd = JinixRuntime.getRuntime().getTranslatorFile();
        if (fd == null) {
            System.err.println("Translator must be started with settrans");
            return;
        }
        try {
            translator = new TimeTranslator(JinixFileChannel.open(fd));
        } catch (IOException e) {
            throw new RuntimeException("Translator failed initialization",e);
        }

        JinixRuntime.getRuntime().bindTranslator(translator);

        mainThread = Thread.currentThread();

        JinixRuntime.getRuntime().registerSignalHandler(new ProcessSignalHandler() {
            @Override
            public void handleSignal(ProcessManager.Signal signal) {
                if (signal == ProcessManager.Signal.TERMINATE) {
                    mainThread.interrupt();
                }
            }
        });

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            // Interrupted shutting down
        }

        translator.unexport();
    }
}
