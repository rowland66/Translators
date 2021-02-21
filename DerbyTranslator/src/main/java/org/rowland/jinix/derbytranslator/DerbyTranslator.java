package org.rowland.jinix.derbytranslator;

import org.apache.derby.iapi.jdbc.AutoloadedDriver;
import org.rowland.jinix.io.BaseRemoteFileHandleImpl;
import org.rowland.jinix.io.JinixFile;
import org.rowland.jinix.io.SimpleDirectoryRemoteFileHandle;
import org.rowland.jinix.lang.JinixRuntime;
import org.rowland.jinix.lang.ProcessSignalHandler;
import org.rowland.jinix.naming.*;
import org.rowland.jinix.proc.ProcessManager;

import javax.naming.NamingException;
import java.net.URI;
import java.nio.file.*;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.*;
import java.util.*;

public class DerbyTranslator extends UnicastRemoteObject implements FileNameSpace, RemoteDataSource {

    static DerbyTranslator instance;

    private static Thread mainThread;

    Driver driver;
    String databaseName;
    FileNameSpace parent;
    String pathWithinParent;

    private String APPSchemaId;

    private static final String ALL_TABLES_SQL = "select TABLENAME from SYS.SYSTABLES where TABLETYPE in ('T','A','V') and SCHEMAID=? order by TABLENAME";
    private static final String TABLE_COUNT_SQL = "select count(*) from SYS.SYSTABLES where TABLETYPE in ('T','A','V') and SCHEMAID=? and TABLENAME=?";

    private static final String[] ROOT_DIRECTORY_NAMES = new String[] {"datasource", "schema", "data"};
    private static final String[] SCHEMA_DIRECTORY_NAMES = new String[] {"tables", "indexes"};
    private static final DirectoryFileData[] ROOT_DIRECTORY_FILE_DATA = new DirectoryFileData[3];

    static {
        for (int i=0; i<ROOT_DIRECTORY_NAMES.length; i++) {
            DirectoryFileData dfd = new DirectoryFileData();
            dfd.name = ROOT_DIRECTORY_NAMES[i];
            dfd.length = 0;
            dfd.type = (i==0 ? DirectoryFileData.FileType.DIRECTORY : DirectoryFileData.FileType.FILE); // datasource is the only file
            dfd.lastModified = 0;
            ROOT_DIRECTORY_FILE_DATA[i] = dfd;
        }
    }
    private DerbyTranslator(String databaseName, FileNameSpace parent, String pathWithinParent) throws RemoteException {

        this.databaseName = databaseName;
        this.parent = parent;
        this.pathWithinParent = pathWithinParent;

        try {
            driver = new AutoloadedDriver();
            Connection conn = driver.connect("jdbc:derby:" + databaseName, null);

            ResultSet schemas = conn.createStatement().executeQuery("select SCHEMAID from SYS.SYSSCHEMAS where SCHEMANAME='APP'");

            if (schemas.next()) {
                APPSchemaId = schemas.getString(1);
            }
            schemas.close();

        } catch (SQLException e) {
            throw new RemoteException("Failure initializing Derby database at "+databaseName, e);
        }
    }

    private void shutdown() {
        try {
            driver.connect("jdbc:derby:;shutdown=true", null);
        } catch (SQLException se) {
            if (( (se.getErrorCode() == 50000)
                    && ("XJ015".equals(se.getSQLState()) ))) {
                // we got the expected exception
                System.out.println("Derby shut down normally");
                // Note that for single database shutdown, the expected
                // SQL state is "08006", and the error code is 45000.
            } else {
                // if the error code or SQLState is different, we have
                // an unexpected exception (shutdown failed)
                System.err.println("Derby did not shut down normally");
                printSQLException(se);
            }
        }
    }

    private static void processInteractive(String databaseName, String[] args) {
        Driver driver;
        Connection conn;
        try {
            driver = new AutoloadedDriver();
            conn = driver.connect("jdbc:derby:" + databaseName + ";create=true", null);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize Derby");
        }

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            System.out.println("Database product name: "+metaData.getDatabaseProductName());
            System.out.println("Database product version: "+metaData.getDatabaseProductVersion());

            if (args.length > 1 && args[1].equals("create")) {
                Statement s = conn.createStatement();
                s.execute("create table location(num int, addr varchar(40))");
                System.out.println("Created table location");
                s.close();

                PreparedStatement psInsert = conn.prepareStatement(
                        "insert into location values (?, ?)");

                psInsert.setInt(1, 1956);
                psInsert.setString(2, "Webster St.");
                psInsert.executeUpdate();
                System.out.println("Inserted 1956 Webster");

                psInsert.setInt(1, 1910);
                psInsert.setString(2, "Union St.");
                psInsert.executeUpdate();
                System.out.println("Inserted 1910 Union");
            }

            if (args.length > 1 && args[1].equals("delete")) {
                Statement s = conn.createStatement();
                s.execute("drop table location");
                System.out.println("Dropped table location");
                s.close();
                return;
            }

        } catch (SQLException e) {
            printSQLException(e);
        } finally {
            try {
                conn.close();
                driver.connect("jdbc:derby:;shutdown=true", null);
            } catch (SQLException se) {
                if (( (se.getErrorCode() == 50000)
                        && ("XJ015".equals(se.getSQLState()) ))) {
                    // we got the expected exception
                    System.out.println("Derby shut down normally");
                    // Note that for single database shutdown, the expected
                    // SQL state is "08006", and the error code is 45000.
                } else {
                    // if the error code or SQLState is different, we have
                    // an unexpected exception (shutdown failed)
                    System.err.println("Derby did not shut down normally");
                    printSQLException(se);
                }
            }
        }

    }

    /**
     * Prints details of an SQLException chain to <code>System.err</code>.
     * Details included are SQL State, Error code, Exception message.
     *
     * @param e the SQLException from which to print details.
     */
    private static void printSQLException(SQLException e)
    {
        // Unwraps the entire exception chain to unveil the real cause of the
        // Exception.
        while (e != null)
        {
            System.err.println("\n----- SQLException -----");
            System.err.println("  SQL State:  " + e.getSQLState());
            System.err.println("  Error Code: " + e.getErrorCode());
            System.err.println("  Message:    " + e.getMessage());
            // for stack traces, refer to derby.log or uncomment this:
            //e.printStackTrace(System.err);
            e = e.getNextException();
        }
    }

    public static void main(String[] args) {

        JinixFile translatorFile = JinixRuntime.getRuntime().getTranslatorFile();

        boolean interactiveMode = false;
        if (translatorFile == null) {
            // Running standalone. use for debugging.
            System.out.println("DerbyTranslator: running in interactive mode");
            interactiveMode = true;
        }

        String translatorNodePath = JinixRuntime.getRuntime().getTranslatorNodePath();

        if (interactiveMode && args.length == 0) {
            System.err.println("DerbyTranslator: database name argument required");
        }

        String databaseName;
        if (interactiveMode) {
            databaseName = args[0];
        } else {
            databaseName = translatorNodePath;
            System.out.println("Starting Derby database: "+databaseName);
        }

        Path root = Paths.get(databaseName).toAbsolutePath();
        databaseName = root.toString();

        if (interactiveMode) {
            processInteractive(databaseName, args);
            return;
        }

        try {
            RemoteFileHandle file = (RemoteFileHandle) (new JinixContext()).lookup(translatorFile.getAbsolutePath());
            instance = new DerbyTranslator(databaseName, file.getParent(), file.getPath());
            JinixRuntime.getRuntime().bindTranslator(instance);
        } catch (NamingException e) {
            throw new RuntimeException("Failed to lookup translator file", e);
        } catch (RemoteException e) {
            throw new RuntimeException("Internal error", e);
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
            e.printStackTrace();
        }

        instance.shutdown();
        try {
            unexportObject(instance, true);
        } catch (NoSuchObjectException e) {
            // Ignore as we are shutting down.
        }
    }

    @Override
    public URI getURI() throws RemoteException {
        return null;
    }

    @Override
    public DirectoryFileData getFileAttributes(String filePath) throws NoSuchFileException, RemoteException {

        if (!exists(filePath)) {
            return null;
        }

        if (filePath.equals("/schema") || filePath.equals("/data")) {
            DirectoryFileData dfd = new DirectoryFileData();
            dfd.name = filePath.substring(1);
            dfd.length = 0;
            dfd.type = DirectoryFileData.FileType.DIRECTORY;
            dfd.lastModified = 0;
            return dfd;
        }

        if (filePath.equals("/datasource")) {
            DirectoryFileData dfd = new DirectoryFileData();
            dfd.name = filePath.substring(1);
            dfd.length = 0;
            dfd.type = DirectoryFileData.FileType.FILE;
            dfd.lastModified = 0;
            return dfd;
        }

        DirectoryFileData dfd = new DirectoryFileData();
        dfd.name = filePath.substring((filePath.equals("/schema") ? "/schema".length() : "/data".length()));
        dfd.length = 0;
        dfd.type = DirectoryFileData.FileType.FILE;
        dfd.lastModified = 0;
        return dfd;
    }

    @Override
    public void setFileAttributes(String s, DirectoryFileData directoryFileData) throws NoSuchFileException, RemoteException {

    }

    private boolean exists(String filePathName) throws InvalidPathException, RemoteException {

        if (filePathName.equals("/")) {
            return true;
        }

        if (filePathName.equals("/datasource")) {
            return true;
        }

        if (filePathName.equals("/schema")) {
            return true;
        }

        if (filePathName.equals("/data")) {
            return true;
        }

        if (filePathName.startsWith("/data/")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                try {
                    PreparedStatement ps = conn.prepareStatement(TABLE_COUNT_SQL);
                    ps.setString(1, APPSchemaId);
                    ps.setString(2, filePathName.substring("/data/".length()));
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        return (rs.getInt(1) > 0);
                    }
                    throw new RuntimeException("Translator Error");
                } finally {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Translator Error", e);
            }
        }

        if (filePathName.startsWith("/schema/")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                try {
                    PreparedStatement ps = conn.prepareStatement(TABLE_COUNT_SQL);
                    ps.setString(1, APPSchemaId);
                    ps.setString(2, filePathName.substring("/data/".length()));
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        return (rs.getInt(1) > 0);
                    }
                    throw new RuntimeException("Translator Error");
                } finally {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Translator Error", e);
            }
        }

        return false;
    }

    @Override
    public String[] list(String s) throws NotDirectoryException, RemoteException {

        if (s.equals("/")) {
            return ROOT_DIRECTORY_NAMES;
        }

        if (s.equals("/data")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                try {
                    List<String> tableNames = new ArrayList<String>();
                    PreparedStatement ps = conn.prepareStatement(ALL_TABLES_SQL);
                    ps.setString(1, APPSchemaId);
                    ResultSet tables = ps.executeQuery();
                    while (tables.next()) {
                        tableNames.add(tables.getString(1));
                    }
                    ps.close();
                    tables.close();

                    return tableNames.toArray(new String[tableNames.size()]);
                } finally {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Translator Error", e);
            }
        }

        if (s.equals("/schema")) {
            return SCHEMA_DIRECTORY_NAMES;
        }

        if (s.equals("/schema/tables")) {
            return new String[0];
        }

        if (s.equals("/schema/indexes")) {
            return new String[0];
        }

        return new String[0];
    }


    @Override
    public boolean createFileAtomically(String directoryPathName, String fileName) throws FileAlreadyExistsException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean createDirectory(String directoryPathName, String directoryName) throws FileAlreadyExistsException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String filePathName) throws NoSuchFileException, DirectoryNotEmptyException, RemoteException {

    }

    @Override
    public void copy(RemoteFileHandle sourcePathName, RemoteFileHandle targetPathName, String file, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, UnsupportedOperationException, RemoteException {

    }

    @Override
    public void move(RemoteFileHandle sourcePathName, RemoteFileHandle targetPathName, String file, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, UnsupportedOperationException, RemoteException {

    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, String filePathName, Set<? extends OpenOption> openOptions) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {

        if (!exists(filePathName)) {
            throw new NoSuchFileException(filePathName);
        }

        if (filePathName.startsWith("/data/")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                return new TableRemoteFileAccessor(this, filePathName.substring("/data/".length()), conn);
            } catch (SQLException e) {
                throw new RemoteException("Translator Error", e);
            }
        }

        if (filePathName.startsWith("/schema/")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                return new SchemaRemoteFileAccessor(filePathName.substring("/schema/".length()), conn);
            } catch (SQLException e) {
                throw new RemoteException("Translator Error", e);
            }
        }

        throw new NoSuchFileException(filePathName);
    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int pid, RemoteFileHandle remoteFileHandle, Set<? extends OpenOption> openOptions) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {
        return getRemoteFileAccessor(pid, remoteFileHandle.getPath(), openOptions);
    }

    @Override
    public Object lookup(int pid, String filePathName) throws RemoteException {

        if (filePathName.equals("/datasource")) {
            return new DataSourceStub(this);
        }

        if (filePathName.equals("/schema")) {
            return new SimpleDirectoryRemoteFileHandle(this, "/schema");
        }

        if (filePathName.equals("/data")) {
            return new SimpleDirectoryRemoteFileHandle(this, "/data");
        }

        if (filePathName.startsWith("/data/")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                try {
                    PreparedStatement ps = conn.prepareStatement(TABLE_COUNT_SQL);
                    ps.setString(1, APPSchemaId);
                    ps.setString(2, filePathName.substring("/data/".length()));
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        return new BaseRemoteFileHandleImpl(this, filePathName);
                    }
                    return null;
                } finally {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Translator Error", e);
            }
        }

        if (filePathName.startsWith("/schema/")) {
            try {
                Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
                try {
                    PreparedStatement ps = conn.prepareStatement(TABLE_COUNT_SQL);
                    ps.setString(1, APPSchemaId);
                    ps.setString(2, filePathName.substring("/schema/".length()));
                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        return new BaseRemoteFileHandleImpl(this, filePathName);
                    }
                    return null;
                } finally {
                    conn.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Translator Error", e);
            }
        }

        return null;
    }

    @Override
    public FileNameSpace getParent() throws RemoteException {
        return parent;
    }

    @Override
    public String getPathWithinParent() throws RemoteException {
        return pathWithinParent;
    }

    @Override
    public List<FileAccessorStatistics> getOpenFiles(int i) throws RemoteException {
        return null;
    }

    // RemoteDataSource interface implementation

    @Override
    public RemoteConnection getConnection(String username, String password) throws RemoteException {
        try {
            Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
            return new DerbyConnection(conn);
        } catch (SQLException e) {
            throw new RemoteException("Translator error", e);
        }
    }
}
