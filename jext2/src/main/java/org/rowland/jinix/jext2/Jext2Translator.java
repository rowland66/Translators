package org.rowland.jinix.jext2;

import jext2.*;
import jext2.exceptions.IoError;
import jext2.exceptions.JExt2Exception;
import jext2.exceptions.NoSuchFileOrDirectory;
import org.rowland.jinix.JinixKernelUnicastRemoteObject;
import org.rowland.jinix.RootFileSystem;
import org.rowland.jinix.io.JinixFile;
import org.rowland.jinix.io.JinixFileInputStream;
import org.rowland.jinix.lang.JinixRuntime;
import org.rowland.jinix.lang.ProcessSignalHandler;
import org.rowland.jinix.naming.*;
import org.rowland.jinix.nio.JinixFileChannel;
import org.rowland.jinix.proc.ProcessManager;

import javax.management.*;
import javax.naming.NamingException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A translator an ext2 file system.
 */
public class Jext2Translator extends JinixKernelUnicastRemoteObject implements FileNameSpace, RootFileSystem {

    private static long EMPTY_LOOKUP = -1;
    private static Jext2Translator translator;
    private static Thread mainThread;

    BlockAccess blocks;
    Superblock superblock;
    BlockGroupAccess blockGroups;
    java.nio.channels.FileChannel blockDev;
    InodeAccess inodes;

    private NameSpace rootNameSpace;
    private RemoteFileHandle rootFile;
    private String blockDevName;

    private Map<Integer, List<FileAccessorStatistics>> openFileMap = Collections.synchronizedMap(
            new HashMap<Integer, List<FileAccessorStatistics>>());

    private Jext2Translator(java.nio.channels.FileChannel blockDev, String blockDevName, RemoteFileHandle translatorFile) throws RemoteException {
        this.rootFile = translatorFile;
        this.blockDev = blockDev;
        this.blockDevName = blockDevName;

        try {
            blocks = new BlockAccess(blockDev);
            superblock = Superblock.fromBlockAccess(blocks);
            blocks.initialize(superblock);

            performBasicFilesystemChecks();
            markExt2AsMounted();
            // TODO set times, volume name, etc in  superblock

            blockGroups = BlockGroupAccess.getInstance();
            blockGroups.readDescriptors();
            inodes = InodeAccess.getInstance();
        } catch (IoError ioError) {
            ioError.printStackTrace();
            return;
        }

        try {
            Inode rootINode = inodes.openInode(Constants.EXT2_ROOT_INO);

            if (!rootINode.isDirectory())
                throw new RuntimeException("lookup: root inode is not a directory");

        } catch (JExt2Exception e) {
            throw new RuntimeException("Cannot open root inode");
        } finally {
            inodes.forgetInode(Constants.EXT2_ROOT_INO, 1);
        }

        Hashtable<String, String> objectNameMap = new Hashtable<>();
        objectNameMap.put("Type", "FileSystem");
        objectNameMap.put("Name", getPathWithinParent());
        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                    new Jext2MBean(this), new ObjectName("org.jinix", objectNameMap));
        } catch (InstanceAlreadyExistsException e) {
            e.printStackTrace();
        } catch (MBeanRegistrationException e) {
            e.printStackTrace();
        } catch (NotCompliantMBeanException e) {
            e.printStackTrace();
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sync() {
        try {
            superblock.sync();
            for (BlockGroupDescriptor descr :blockGroups.iterateBlockGroups()) {
                descr.sync();
            }
        } catch (IoError ioError) {
            Filesystem.getLogger().log(Level.SEVERE, "IO Failure", ioError);
        }
    }

    @Override
    public void shutdown() {
        sync();
    }

    @Override
    public URI getURI() {
        try {
            String path = this.blockDevName.replace('\\', '/');
            if (!path.startsWith("/")) {
                path = path.substring(path.indexOf('/'),path.length());
            }
            return new URI("file", null, path, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unexpected failure creating FileNameSpace URI", e);
        }
    }

    @Override
    public Object lookup(int pid, String pathName) throws RemoteException {

        if (!pathName.isEmpty() && !pathName.startsWith("/")) {
            throw new IllegalArgumentException("Lookup path must begin with slash: "+pathName);
        }

        // Remove any trailing '/' characters
        while (pathName.endsWith("/")) {
            pathName = pathName.substring(0,pathName.length()-1);
        }

        try {
            long ino = lookupInternal(null, pathName);
            if (ino == EMPTY_LOOKUP) {
                return null;
            }
            return new Jext2RemoteFileHandle(this, pathName, ino);
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2 Failure", e);
        }
    }

    private long lookupInternal(DirectoryInode parentInode, String pathName) throws JExt2Exception {

        if (parentInode == null) {

            if (pathName.length() == 0) {
                return Constants.EXT2_ROOT_INO;
            }

            try {
                parentInode = (DirectoryInode) inodes.openInode(Constants.EXT2_ROOT_INO);

                return lookupInternal((DirectoryInode) parentInode, pathName);
            } finally {
                inodes.forgetInode(parentInode.getIno(), 1);
            }
        }

        pathName = pathName.substring(1); // Remove the leading '/'

        if (pathName.isEmpty()) {
            return parentInode.getIno();
        }

        String localName = pathName;
        if (localName.indexOf('/') > 0) {
            localName = localName.substring(0, localName.indexOf('/'));
            pathName = pathName.substring(pathName.indexOf('/'));
        } else {
            pathName = null;
        }

        try {
            DirectoryEntry entry = parentInode.lookup(localName);

            try {
                if (pathName == null) {
                    return entry.getIno();
                } else {
                    if (entry.isDirectory()) {
                        parentInode = (DirectoryInode) inodes.openInode(entry.getIno());
                        try {
                            return lookupInternal(parentInode, pathName);
                        } finally {
                            inodes.forgetInode(parentInode.getIno(), 1);
                        }
                    } else {
                        return EMPTY_LOOKUP;
                    }
                }
            } finally {
                parentInode.directoryEntries.release(entry);
            }
        } catch (NoSuchFileOrDirectory e) {
            return EMPTY_LOOKUP;
        }
    }

    @Override
    public DirectoryFileData getFileAttributes(String pathName) throws NoSuchFileException, RemoteException {

        if (!pathName.isEmpty() && !pathName.startsWith("/")) {
            throw new IllegalArgumentException("Lookup path must begin with slash: "+pathName);
        }

        // Remove any trailing '/' characters
        while (pathName.endsWith("/")) {
            pathName = pathName.substring(0,pathName.length()-1);
        }

        try {
            long ino = Constants.EXT2_ROOT_INO;
            String fileName = "/";

            ino = lookupInternal(null, pathName);
            if (ino == EMPTY_LOOKUP) {
                throw new NoSuchFileException(pathName);
            }
            return getFileAttributes(pathName, ino);
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2Exception in getFileAttributes()");
        }
    }

    public DirectoryFileData getFileAttributes(String pathName, long ino) throws NoSuchFileException, RemoteException {

        String fileName = pathName.substring(pathName.lastIndexOf('/')+1);
        try {
            Inode inode = (Inode)(inodes.openInode(ino));
            try {
                if (inode != null) {
                    DirectoryFileData dfd = new DirectoryFileData();
                    dfd.length = inode.getSize();
                    dfd.type = (inode.getFileType() == DirectoryEntry.FILETYPE_DIR ?
                            DirectoryFileData.FileType.DIRECTORY :
                            DirectoryFileData.FileType.FILE);
                    dfd.lastModified = inode.getModificationTime().getTime();
                    return dfd;
                } else {
                    throw new NoSuchFileException(pathName);
                }
            } finally {
                inodes.forgetInode(inode.getIno(), 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2Exception in getFileAttributes()");
        }
    }

    @Override
    public void setFileAttributes(String name, DirectoryFileData directoryFileData) throws NoSuchFileException, RemoteException {

    }

    @Override
    public String[] list(String pathName) throws RemoteException {
        if (!pathName.isEmpty() && !pathName.startsWith("/")) {
            throw new IllegalArgumentException("Lookup path must begin with slash: "+pathName);
        }

        // Remove any trailing '/' characters
        while (pathName.endsWith("/")) {
            pathName = pathName.substring(0,pathName.length()-1);
        }

        try {
            long ino = lookupInternal(null, pathName);
            if (ino == EMPTY_LOOKUP) {
                return null;
            }

            Inode inode = inodes.openInode(ino);
            try {
                if (!(inode instanceof DirectoryInode)) {
                    return null;
                }
                List<String> dir = new ArrayList<String>(20);
                DirectoryInode directory = (DirectoryInode) inode;
                DirectoryInode.DirectoryIterator di = directory.iterateDirectory();
                while (di.hasNext()) {
                    DirectoryEntry entry = di.next();
                    dir.add(entry.getName());
                }
                String[] rtrn = new String[dir.size()];
                dir.toArray(rtrn);
                return rtrn;
            } finally {
                inodes.forgetInode(inode.getIno(), 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2Exception in list()");
        }
    }

    @Override
    public boolean createFileAtomically(String directoryName, String fileName) throws FileAlreadyExistsException, RemoteException {

        try {
            long parentDirInode = lookupInternal(null, directoryName);
            if (parentDirInode == EMPTY_LOOKUP) {
                return false;
            }

            Inode parentInode = inodes.openInode(parentDirInode);
            try {
                if (!parentInode.isDirectory())
                    return false;

                RegularInode inode = RegularInode.createEmpty();
                inode.setMode(new ModeBuilder()
                        .regularFile()
                        .create());
                InodeAlloc.registerInode(parentInode, inode);

                ((DirectoryInode) parentInode).addLink(inode, fileName);
                inode.sync();

                return true;
            } finally {
                inodes.forgetInode(parentInode.getIno(), 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in createDirectory()");
        }
    }

    @Override
    public boolean createDirectory(String parentDirectory, String directoryName) throws FileAlreadyExistsException, RemoteException {

        try {
            long parentDirInode = lookupInternal(null, parentDirectory);
            if (parentDirInode == EMPTY_LOOKUP) {
                return false;
            }

            Inode parentInode = inodes.openInode(parentDirInode);
            try {
                if (!parentInode.isDirectory()) {
                    return false;
                }
                DirectoryInode inode = DirectoryInode.createEmpty();
                inode.setMode(new ModeBuilder()
                        .directory()
                        .create());
                InodeAlloc.registerInode(parentInode, inode);
                inode.addDotLinks((DirectoryInode) parentInode);

                ((DirectoryInode) parentInode).addLink(inode, directoryName);
                inode.sync();

                return true;
            } finally {
                inodes.forgetInode(parentInode.getIno(), 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in createDirectory()");
        }
    }

    @Override
    public void delete(String pathName) throws NoSuchFileException, DirectoryNotEmptyException, RemoteException {
        try {
            long ino = lookupInternal(null, pathName);
            if (ino == EMPTY_LOOKUP) {
                throw new NoSuchFileException(pathName);
            }

            Inode inode = inodes.openInode(ino);
            try {
                if (inode instanceof DirectoryInode) {
                    if (!((DirectoryInode) inode).isEmptyDirectory()) {
                        throw new DirectoryNotEmptyException(pathName);
                    }
                }
            } finally {
                inodes.forgetInode(ino, 1);
            }

            if (ino == Constants.EXT2_ROOT_INO) {
                throw new RemoteException("Illegal attempt to delete root directory");
            }

            String dirPath = pathName.substring(0, pathName.lastIndexOf('/'));
            String fileName = pathName.substring(pathName.lastIndexOf('/')+1);
            long directoryIno = lookupInternal(null, dirPath);
            DirectoryInode dirInode = (DirectoryInode) inodes.openInode(directoryIno);
            try {
                dirInode.removeDirectoryEntry(fileName);
                inode.delete();
            } finally {
                inodes.forgetInode(directoryIno, 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in delete()", e);
        }
    }

    @Override
    public void copy(RemoteFileHandle sourceFile, RemoteFileHandle destinationDirectory, String fileName, CopyOption... options) throws NoSuchFileException, FileAlreadyExistsException, RemoteException {
        RemoteFileAccessor sourceFileAccessor = getRemoteFileAccessor(0, sourceFile, EnumSet.of(StandardOpenOption.READ));
        try {
            RemoteFileAccessor destinationFileAccessor = destinationDirectory.getParent().getRemoteFileAccessor(0,
                    destinationDirectory.getPath() + "/" + fileName,
                    EnumSet.of(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW));
            try {
                byte[] buffer = sourceFileAccessor.read(0, 2048);
                while (buffer != null) {
                    destinationFileAccessor.write(0, buffer);
                    buffer = sourceFileAccessor.read(0, 2048);
                }
            } finally {
                destinationFileAccessor.close();
            }
        } finally {
            sourceFileAccessor.close();
        }
    }

    @Override
    public void move(RemoteFileHandle sourceFile, RemoteFileHandle destinationDirectory, String fileName, CopyOption... options) throws IOException {

        if (destinationDirectory.getParent().getURI().equals(this.getURI())) {
            String sourceFilePathName = ((RemoteFileHandle) sourceFile).getPath();
            String sourceFileName = sourceFilePathName.substring(sourceFilePathName.lastIndexOf('/')+1);
            String sourceFileDirectory = sourceFilePathName.substring(0, sourceFilePathName.lastIndexOf('/'));
            long destinationDirectoryIno = ((Jext2RemoteFileHandle) destinationDirectory).getInodeOffset();

            try {
                try {
                    if (!inodes.openInode(destinationDirectoryIno).isDirectory()) {
                        throw new NotDirectoryException(destinationDirectory.getPath());
                    }
                } finally {
                    inodes.forgetInode(destinationDirectoryIno, 1);
                }
                long sourceFileDirectoryIno = lookupInternal(null, sourceFileDirectory);
                rename(sourceFileDirectoryIno, sourceFileName, destinationDirectoryIno, fileName);
            } catch (JExt2Exception e) {
                throw new RemoteException("IOException moving file " + sourceFilePathName + " to " + destinationDirectory + "/" + fileName);
            }
        } else {
            copy(sourceFile, destinationDirectory, fileName, options);
        }
    }

    private void rename(long parent, String name, long newparent, String newname) throws JExt2Exception {

        if (parent == 1) parent = Constants.EXT2_ROOT_INO;
        if (newparent == 1) newparent = Constants.EXT2_ROOT_INO;

        DirectoryInode parentInode;
        DirectoryInode newparentInode;

        parentInode = (DirectoryInode) inodes.openInode(parent);
        try {
            newparentInode = (DirectoryInode) inodes.openInode(newparent);
            try {

                if (parentInode.equals(newparentInode)) {
                    parentInode.directoryLock().writeLock().lock();
                } else {
                    parentInode.directoryLock().writeLock().lock();
                    newparentInode.directoryLock().writeLock().lock();
                }

                try {
                    DirectoryEntryAccess newparentEntries = newparentInode.directoryEntries;

                    /* create entries */
                    DirectoryEntry entry = parentInode.lookup(name);

                    DirectoryEntry newEntry = DirectoryEntry.create(newname);
                    newEntry.setIno(entry.getIno());
                    newEntry.setFileType(entry.getFileType());

                    parentInode.directoryEntries.release(entry);

                    /*
                     * When NEW directory entry already exists try to
                     * delete it.
                     */
                    try {
                        DirectoryEntry existingEntry = newparentInode.lookup(newname);
                        if (existingEntry.isDirectory()) {
                            DirectoryInode existingDir =
                                    (DirectoryInode) inodes.getOpened(existingEntry.getIno());

                            newparentInode.unLinkDir(existingDir, newname);
                        } else {
                            Inode existing = inodes.getOpened(existingEntry.getIno());
                            newparentInode.unLinkOther(existing, newname);
                        }
                        // newparentInode.directoryEntries.release(existingEntry); Don't think that we need this.
                    } catch (NoSuchFileOrDirectory ignored) {
                    }

                    /*
                     * When we move a directory we need to update the dot-dot entry
                     * and the nlinks of the parents.
                     */
                    if (newEntry.isDirectory()) {
                        DirectoryInode newDir =
                                (DirectoryInode) inodes.openInode(newEntry.getIno()); // ?? not open?

                        try {
                            DirectoryEntry dotdot = newDir.lookup("..");
                            dotdot.setIno(newparentInode.getIno());
                            dotdot.sync();

                            newparentInode.setLinksCount(newparentInode.getLinksCount() + 1);
                            parentInode.setLinksCount(parentInode.getLinksCount() - 1);

                            newDir.directoryEntries.release(dotdot);
                        } finally {
                            inodes.forgetInode(newEntry.getIno(), 1);
                        }
                    }

                    /*
                     * Finally: Change the Directories
                     */
                    newparentInode.addDirectoryEntry(newEntry);
                    parentInode.removeDirectoryEntry(name);

                } finally {
                    assert parentInode.directoryLock().getWriteHoldCount() == 1;
                    assert newparentInode.directoryLock().getWriteHoldCount() == 1;

                    if (parentInode.equals(newparentInode)) {
                        parentInode.directoryLock().writeLock().unlock();
                    } else {
                        parentInode.directoryLock().writeLock().unlock();
                        newparentInode.directoryLock().writeLock().unlock();
                    }
                }
            } finally {
                inodes.forgetInode(newparent, 1);
            }
        } finally {
            inodes.forgetInode(parent, 1);
        }
    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, String name, Set<? extends OpenOption> openOptions) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {

        if (name.equals("/")) {
            throw new RemoteException("Illegal get file channel for root directory");
        }

        if (openOptions == null) {
            openOptions = Collections.emptySet();
        }

        try {
            long fileIno = lookupInternal(null, name);
            if (fileIno != EMPTY_LOOKUP) {

                if (openOptions.contains(StandardOpenOption.CREATE_NEW)) {
                    throw new FileAlreadyExistsException(name);
                }

                return getRemoteFileAccessor(pid, name, fileIno, openOptions);
            }

            if (!(openOptions.contains(StandardOpenOption.CREATE) ||
                    openOptions.contains(StandardOpenOption.CREATE_NEW))) {
                throw new NoSuchFileException(name);
            }

            String dirPath = name.substring(0, name.lastIndexOf('/'));
            long dirIno = lookupInternal(null, dirPath);
            if (dirIno == EMPTY_LOOKUP) {
                throw new NoSuchFileException(name);
            }

            Inode parentInode = inodes.openInode(dirIno);
            try {
                if (!parentInode.isDirectory()) {
                    throw new NoSuchFileException(name);
                }

                String newFileName = name.substring(name.lastIndexOf('/')+1);

                RegularInode inode = RegularInode.createEmpty();
                inode.setMode(new ModeBuilder()
                        .regularFile()
                        .create());
                InodeAlloc.registerInode(parentInode, inode);

                ((DirectoryInode) parentInode).addLink(inode, newFileName);
                inode.sync();

                return getRemoteFileAccessor(pid, name, inode.getIno(), openOptions);
            } finally {
                inodes.forgetInode(parentInode.getIno(), 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in getFileChannel()");
        }
    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, RemoteFileHandle remoteFileHandle, Set<? extends OpenOption> openOptions)
            throws FileAlreadyExistsException, NoSuchFileException, RemoteException {
        return getRemoteFileAccessor(pid, remoteFileHandle.getPath(), ((Jext2RemoteFileHandle) remoteFileHandle).getInodeOffset(), openOptions);
    }

    private RemoteFileAccessor getRemoteFileAccessor(int pid, String pathName, long ino, Set<? extends OpenOption> openOptions)
            throws FileAlreadyExistsException, NoSuchFileException, RemoteException {

        if (openOptions == null) {
            openOptions = Collections.emptySet();
        }

        List<FileAccessorStatistics> l = openFileMap.get(pid);
        if (l == null) {
            l = new LinkedList<FileAccessorStatistics>();
            openFileMap.put(pid, l);
        }

        Jext2ChannelServer s = new Jext2ChannelServer(this, pid, pathName, ino, openOptions);
        l.add(s);

        return s;
    }

    @Override
    public List<FileAccessorStatistics> getOpenFiles(int pid) throws RemoteException {
        return this.openFileMap.get(pid);
    }

    @Override
    public FileNameSpace getParent() throws RemoteException {
        return rootFile.getParent();
    }

    @Override
    public String getPathWithinParent() throws RemoteException {
        return rootFile.getPath();
    }

    void removeFileSystemChannelServer(int pid, Jext2ChannelServer s) {
        List<FileAccessorStatistics> l = openFileMap.get(pid);
        l.remove(s);
    }

    private void performBasicFilesystemChecks()
    {
        checkExt2Features();
        checkExt2Magic();
        checkExt2Revision();
        checkExt2MountState();
    }

    private void checkExt2Features() {
        if (Feature.incompatUnsupported() || Feature.roCompatUnsupported()) {
            System.out.println("Featureset incompatible with JExt2 :(");
            System.exit(23);
        }
    }

    private void checkExt2Magic() {
        if (superblock.getMagic() != 0xEF53) {
            System.out.println("Wrong magic -> no ext2");
            System.exit(23);
        }
    }

    private void checkExt2Revision() {
		/* ext2_setup_super */
        if (superblock.getRevLevel() > Constants.JEXT2_MAX_SUPP_REV) {
            System.out.println("Error: Revision level too high, exiting");
            System.exit(23);
        }
    }

    private void checkExt2MountState() {
        if ((superblock.getState() & Constants.EXT2_VALID_FS) == 0)
            System.out.println("Mounting uncheckt fs");
        else if ((superblock.getState() & Constants.EXT2_ERROR_FS) > 0)
            System.out.println("Mounting fs with errors");
        else if ((superblock.getMaxMountCount() >= 0) &&
                (superblock.getMountCount() >= superblock.getMaxMountCount()))
            System.out.println("Maximal mount count reached");

        if (superblock.getMaxMountCount() == 0)
            superblock.setMaxMountCount(Constants.EXT2_DFL_MAX_MNT_COUNT);
    }

    private void markExt2AsMounted() {
        superblock.setMountCount(superblock.getMountCount() + 1);
        superblock.setLastMount(new Date());
        superblock.setLastMounted("jext2");
    }

    public static void main(String[] args) {

        JinixFile translatorFile = JinixRuntime.getRuntime().getTranslatorFile();
        if (translatorFile == null) {
            // Running standalone. use for debugging.
            System.err.println("Jext2Translator: must run as translator");
            return;
        }

        String blockDevName = null;
        Set<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
        java.nio.channels.FileChannel blockDev = null;
        try {
            if (args.length > 0) {
                Path root = Paths.get(args[0]).toAbsolutePath();
                System.out.println("Starting Jext2 Translator on device: "+root.toString());
                blockDev = java.nio.channels.FileChannel.open(root, options);
                blockDevName = root.toString();
            } else {
                System.out.println("Starting Jext2 Translator on file: "+translatorFile.getAbsolutePath());
                JinixFileInputStream jis = new JinixFileInputStream(translatorFile);
                blockDev = JinixFileChannel.open(jis.getFD(), options, null);
                blockDevName = JinixRuntime.getRuntime().getTranslatorNodePath();
            }
        }
        catch (IOException e) {
            return;
        }

        Filesystem.setLogger(Logger.getLogger("jext2"));
        Filesystem.setLogLevel("FINEST");

        try {
            RemoteFileHandle translatorFileRemoteFileHandle = (RemoteFileHandle) (new JinixContext()).lookup(translatorFile.getAbsolutePath());
            translator = new Jext2Translator(blockDev, blockDevName, translatorFileRemoteFileHandle);
            JinixRuntime.getRuntime().bindTranslator(translator);
        } catch (RemoteException e) {
            throw new RuntimeException("Internal error", e);
        } catch (NamingException e) {
            throw new RuntimeException("Failed to lookup translator node", e);
        }

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
            Filesystem.getLogger().log(Level.INFO, "TERMINATE signal received. Shutting down.");
        }

        translator.shutdown();

        translator.unexport();

        Filesystem.getLogger().log(Level.INFO, "Jext2 Filesystem shutdown complete: "+translator.getURI().toString());
    }
}
