package org.rowland.jinix.jext2;

import jext2.*;
import jext2.exceptions.IoError;
import jext2.exceptions.JExt2Exception;
import jext2.exceptions.NoSuchFileOrDirectory;
import org.apache.commons.lang3.StringUtils;
import org.rowland.jinix.io.JinixFileDescriptor;
import org.rowland.jinix.lang.JinixRuntime;
import org.rowland.jinix.naming.*;
import org.rowland.jinix.nio.JinixFileChannel;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.*;
import java.nio.file.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A translator an ext2 file system.
 */
public class Jext2Translator extends UnicastRemoteObject implements FileNameSpace {

    static Logger logger = Logger.getLogger("jinix.jext2FileSystem");
    private static LookupResult EMPTY_LOOKUP = new LookupResult(null, -1);
    private static Jext2Translator translator;

    BlockAccess blocks;
    Superblock superblock;
    BlockGroupAccess blockGroups;
    java.nio.channels.FileChannel blockDev;
    InodeAccess inodes;

    private NameSpace rootNameSpace;
    private JinixFileDescriptor rootFile;
    private String fsName;

    private Map<Integer, List<FileAccessorStatistics>> openFileMap = Collections.synchronizedMap(
            new HashMap<Integer, List<FileAccessorStatistics>>());

    private Jext2Translator(java.nio.channels.FileChannel blockDev, JinixFileDescriptor fd, String fsName) throws RemoteException {
        rootFile = fd;
        this.fsName = fsName;
        this.rootNameSpace = JinixRuntime.getRuntime().getRootNamespace();
        this.blockDev = blockDev;

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
            inodes.openInode(Constants.EXT2_ROOT_INO);
        } catch (JExt2Exception e) {
            throw new RuntimeException("Cannot open root inode");
        }

        try {
            Inode rootINode = inodes.openInode(Constants.EXT2_ROOT_INO);

            if (!rootINode.isDirectory())
                throw new RemoteException("lookup: root inode is not a directory");

        } catch (JExt2Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            throw new RemoteException("Failure creating J2Ext translator");
        }
    }

    @Override
    public URI getURI() throws RemoteException {
        try {
            String path = this.fsName.replace('\\', '/');
            if (!path.startsWith("/")) {
                path = path.substring(path.indexOf('/'),path.length());
            }
            return new URI("file", null, path, null);
        } catch (URISyntaxException e) {
            throw new RemoteException("Unexpected failure creating FileNameSpace URI", e);
        }
    }

    private LookupResult lookup(String pathName) throws JExt2Exception {

        if (!pathName.isEmpty() && !pathName.startsWith("/")) {
            throw new IllegalArgumentException("Lookup path must begin with slash: "+pathName);
        }

        long parent = Constants.EXT2_ROOT_INO;

        if (pathName.isEmpty() || pathName.equals("/")) {
            return new LookupResult(pathName,parent);
        }

        // Remove any trailing '/' characters
        while (pathName.endsWith("/")) {
            pathName = pathName.substring(0,pathName.length()-1);
        }

        if (pathName.length() == 0) {
            return new LookupResult(pathName,parent);
        }

        Inode parentInode = null;
        try {
            parentInode = inodes.openInode(parent);

            return lookupInternal((DirectoryInode) parentInode, pathName);
        } finally {
            inodes.forgetInode(parentInode.getIno(), 1);
        }
    }

    private LookupResult lookupInternal(DirectoryInode parentInode, String pathName) throws JExt2Exception {

        pathName = pathName.substring(1); // Remove the leading '/'

        String localName = pathName;
        if (localName.indexOf('/') > 0) {
            localName = localName.substring(0, localName.indexOf('/'));
            pathName = pathName.substring(pathName.indexOf('/'));
        } else {
            pathName = null;
        }

        DirectoryEntry entry = null;
        try {
            entry = parentInode.lookup(localName);
            parentInode.directoryEntries.release(entry);

            if (pathName == null) {
                return new LookupResult(localName, entry.getIno());
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
        } catch (NoSuchFileOrDirectory e) {
            return EMPTY_LOOKUP;
        }
    }

    private static class LookupResult {
        final String name;
        final long ino;

        private LookupResult(String name, long ino) {
            this.name = name;
            this.ino = ino;
        }
    }

    @Override
    public DirectoryFileData getFileAttributes(String name) throws NoSuchFileException, RemoteException {

        try {
            long ino = Constants.EXT2_ROOT_INO;
            String fileName = "/";

            if (!name.equals("/")) {
                LookupResult lr = lookup(name);
                ino = lr.ino;
                if (ino == -1) {
                    throw new NoSuchFileException(name);
                }
                fileName = lr.name;
            }

            Inode inode = (Inode)(inodes.openInode(ino));
            try {
                if (inode != null) {
                    DirectoryFileData dfd = new DirectoryFileData();
                    dfd.name = fileName;
                    dfd.length = inode.getSize();
                    dfd.type = (inode.getFileType() == DirectoryEntry.FILETYPE_DIR ?
                            DirectoryFileData.FileType.DIRECTORY :
                            DirectoryFileData.FileType.FILE);
                    dfd.lastModified = inode.getModificationTime().getTime();
                    return dfd;
                } else {
                    throw new NoSuchFileException(name);
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
    public boolean exists(String name) throws RemoteException {
        try {
            LookupResult lr = lookup(name);
            long ino = lr.ino;
            return (ino != -1);
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2Exception in exists()");
        }
    }

    @Override
    public String[] list(String name) throws RemoteException {
        try {
            LookupResult lr = lookup(name);
            long ino = lr.ino;
            if (ino == -1) {
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
    public DirectoryFileData[] listDirectoryFileData(String name) throws RemoteException {
        try {
            LookupResult lr = lookup(name);
            long ino = lr.ino;
            if (ino == -1) {
                throw null;
            }
            Inode inode = inodes.openInode(ino);
            try {
                if (!(inode instanceof DirectoryInode)) {
                    return null;
                }
                List<DirectoryFileData> dir = new ArrayList<DirectoryFileData>(20);
                DirectoryInode directory = (DirectoryInode) inode;
                DirectoryInode.DirectoryIterator di = directory.iterateDirectory();
                while (di.hasNext()) {
                    DirectoryEntry entry = di.next();
                    DirectoryFileData dfd = new DirectoryFileData();
                    dfd.name = entry.getName();
                    dfd.length = entry.getRecLen();
                    dfd.type = (entry.isDirectory() ? DirectoryFileData.FileType.DIRECTORY : DirectoryFileData.FileType.FILE);
                    dfd.lastModified = System.currentTimeMillis();
                    dir.add(dfd);
                }
                DirectoryFileData[] rtrn = new DirectoryFileData[dir.size()];
                dir.toArray(rtrn);
                return rtrn;
            } finally {
                inodes.forgetInode(inode.getIno(), 1);
            }
        } catch (JExt2Exception e) {
            throw new RemoteException("JExt2Exception in listDirectoryFileData()");
        }
    }

    @Override
    public boolean createFileAtomically(String name) throws FileAlreadyExistsException, RemoteException {

        String parentDir = name.substring(0, (name.lastIndexOf('/') == 0 ? 1 : name.lastIndexOf('/')));
        String fileName = name.substring(name.lastIndexOf('/')+1, name.length());

        try {
            LookupResult lr = lookup(parentDir);
            long parentDirInode = lr.ino;
            if (parentDirInode == -1) {
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
    public boolean createDirectory(String name) throws FileAlreadyExistsException, RemoteException {

        String parentDir = name.substring(0, (name.lastIndexOf('/') == 0 ? 1 : name.lastIndexOf('/')));
        String dirName = name.substring(name.lastIndexOf('/')+1, name.length());

        try {
            LookupResult lr = lookup(parentDir);
            long parentDirInode = lr.ino;
            if (parentDirInode == -1) {
                return false;
            }

            Inode parentInode = inodes.openInode(parentDirInode);
            if (!parentInode.isDirectory())
                return false;

            try {
                DirectoryInode inode = DirectoryInode.createEmpty();
                inode.setMode(new ModeBuilder()
                        .directory()
                        .create());
                InodeAlloc.registerInode(parentInode, inode);
                inode.addDotLinks((DirectoryInode) parentInode);

                ((DirectoryInode) parentInode).addLink(inode, dirName);
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
    public boolean createDirectories(String name) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public void delete(String name) throws NoSuchFileException, DirectoryNotEmptyException, RemoteException {
        try {
            LookupResult lr = lookup(name);
            long ino = lr.ino;
            if (ino == -1) {
                throw new NoSuchFileException(name);
            }

            Inode inode = inodes.openInode(ino);
            try {
                if (inode instanceof DirectoryInode) {
                    if (!((DirectoryInode) inode).isEmptyDirectory()) {
                        throw new DirectoryNotEmptyException(name);
                    }
                }
            } finally {
                inodes.forgetInode(ino, 1);
            }

            if (name.equals("/")) {
                throw new RemoteException("Illegal attempt to delete root directory");
            }

            String dirPath = name.substring(0, name.lastIndexOf('/'));
            String fileName = name.substring(name.lastIndexOf('/')+1);
            LookupResult dirLr = lookup(dirPath);
            DirectoryInode dirInode = (DirectoryInode) inodes.openInode(dirLr.ino);
            try {
                dirInode.removeDirectoryEntry(fileName);
                inode.delete();
            } finally {
                inodes.forgetInode(dirLr.ino, 1);
            }
        } catch (JExt2Exception e) {
            logger.log(Level.SEVERE, "Internal error in delete()", e);
            throw new RemoteException("Internal error in delete()");
        }
    }

    @Override
    public void copy(String source, String destination, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void move(String source, String destination, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, RemoteException {

        if (!destination.startsWith("/")) {
            throw new IllegalArgumentException("Move pathNameTo must begin with slash: "+destination);
        }

        org.rowland.jinix.naming.LookupResult result = rootNameSpace.lookup(destination);
        if (!((FileNameSpace) result.remote).getURI().equals(getURI())) {
            throw new UnsupportedOperationException();
        }
        destination = result.remainingPath; // This is be the destination path relative to this file system

        if (exists(destination) && getFileAttributes(destination).type == DirectoryFileData.FileType.DIRECTORY) {
            throw new FileAlreadyExistsException(destination);
        }

        String sourceDir = source.substring(0, (source.lastIndexOf('/') == 0 ? 1 : source.lastIndexOf('/')));
        String sourceName = source.substring(source.lastIndexOf('/')+1, source.length());
        String destinationDir = destination.substring(0, (destination.lastIndexOf('/') == 0 ? 1 : destination.lastIndexOf('/')));
        String destinationName = destination.substring(destination.lastIndexOf('/')+1, destination.length());

        long sourceInode;
        long destinationInode;
        try {
            LookupResult lr = lookup(sourceDir);
            sourceInode = lr.ino;
            if (sourceInode == -1) {
                throw new NoSuchFileException(source);
            }

            lr = lookup(destinationDir);
            destinationInode = lr.ino;
            if (destinationInode == -1) {
                throw new NoSuchFileException(source);
            }

            rename(sourceInode, sourceName, destinationInode, destinationName);

        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in move()");
        }
    }

    private void rename(long parent, String name, long newparent, String newname) throws JExt2Exception {

        if (parent == 1) parent = Constants.EXT2_ROOT_INO;
        if (newparent == 1) newparent = Constants.EXT2_ROOT_INO;

        ReentrantReadWriteLock parentLock = null;
        ReentrantReadWriteLock newparentLock = null;

        DirectoryInode parentInode;
        DirectoryInode newparentInode;

        parentInode = (DirectoryInode) inodes.openInode(parent);
        newparentInode = (DirectoryInode) inodes.openInode(newparent);

        parentLock = parentInode.directoryLock();
        newparentLock = newparentInode.directoryLock();

        if (parentInode.equals(newparentInode)) {
            parentLock.writeLock().lock();
        } else {
            parentLock.writeLock().lock();
            newparentLock.writeLock().lock();
        }

        try {
            DirectoryEntryAccess parentEntries = ((DirectoryInode)parentInode).directoryEntries;
            DirectoryEntryAccess newparentEntries = ((DirectoryInode)newparentInode).directoryEntries;

			/* create entries */
            DirectoryEntry entry = parentInode.lookup(name);

            DirectoryEntry newEntry = DirectoryEntry.create(newname);
            newEntry.setIno(entry.getIno());
            newEntry.setFileType(entry.getFileType());

            parentEntries.release(entry);

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
                newparentEntries.release(existingEntry);
            } catch (NoSuchFileOrDirectory ignored) {
            }

			/*
			 * When we move a directory we need to update the dot-dot entry
			 * and the nlinks of the parents.
			 */
            if (newEntry.isDirectory()) {
                DirectoryInode newDir =
                        (DirectoryInode)inodes.openInode(newEntry.getIno()); // ?? not open?

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
            assert parentLock.getWriteHoldCount() == 1;
            assert newparentLock.getWriteHoldCount() == 1;

            if (parentLock.equals(newparentLock)) {
                parentLock.writeLock().unlock();
            } else {
                parentLock.writeLock().unlock();
                newparentLock.writeLock().unlock();
            }
        }
    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, String name, Set<? extends OpenOption> openOptions) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {
        try {
            if (name.equals("/")) {
                throw new RemoteException("Illegal get file channel for root directory");
            }

            String dirPath = name.substring(0, name.lastIndexOf('/')+1);
            LookupResult dirLr = lookup(dirPath);
            if (dirLr.ino == -1) {
                throw new NoSuchFileException(name);
            }
            String localName = name.substring(name.lastIndexOf('/')+1);

            LookupResult lr = lookup(name);
            long ino = lr.ino;

            List<FileAccessorStatistics> l = openFileMap.get(pid);
            if (l == null) {
                l = new LinkedList<FileAccessorStatistics>();
                openFileMap.put(pid, l);
            }

            Jext2ChannelServer s = new Jext2ChannelServer(this, pid, localName, dirLr.ino, ino, openOptions);
            l.add(s);

            return s;
        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in getFileChannel()");
        }
    }

    @Override
    public Object getKey(String name) throws RemoteException {
        try {
            LookupResult lr = lookup(name);
            long ino = lr.ino;
            if (ino == -1) {
                return null;
            }
            return new Long(ino);
        } catch (JExt2Exception e) {
            throw new RemoteException("Internal error in getKey()");
        }
    }

    @Override
    public List<FileAccessorStatistics> getOpenFiles(int pid) throws RemoteException {
        return this.openFileMap.get(pid);
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

        JinixFileDescriptor fd = JinixRuntime.getRuntime().getTranslatorFile();
        if (fd == null) {
            // Running standalone. use for debugging.
            System.err.println("Jext2Translator: must run as translator");
            return;
        }

        String fsName = null;
        Set<StandardOpenOption> options = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
        java.nio.channels.FileChannel blockDev = null;
        try {
            if (args.length > 0) {
                Path root = Paths.get(args[0]).toAbsolutePath();
                blockDev = java.nio.channels.FileChannel.open(root, options);
                fsName = root.toString();
            } else {
                blockDev = JinixFileChannel.open(fd, options, null);
                fsName = JinixRuntime.getRuntime().getTranslatorNodePath();
            }
        }
        catch (IOException e) {
            return;
        }

        Filesystem.setLogger(Logger.getLogger("jext2"));
        Filesystem.setLogLevel("FINEST");

        try {
            translator = new Jext2Translator(blockDev, fd, fsName);
            JinixRuntime.getRuntime().bindTranslator(translator);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
