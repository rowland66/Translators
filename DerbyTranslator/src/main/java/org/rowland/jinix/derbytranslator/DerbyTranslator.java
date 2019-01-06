package org.rowland.jinix.derbytranslator;

import org.apache.derby.jdbc.AutoloadedDriver;
import org.rowland.jinix.io.JinixFileDescriptor;
import org.rowland.jinix.lang.JinixRuntime;
import org.rowland.jinix.lang.ProcessSignalHandler;
import org.rowland.jinix.naming.DirectoryFileData;
import org.rowland.jinix.naming.FileAccessorStatistics;
import org.rowland.jinix.naming.FileNameSpace;
import org.rowland.jinix.naming.RemoteFileAccessor;
import org.rowland.jinix.proc.ProcessManager;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.*;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class DerbyTranslator extends UnicastRemoteObject implements FileNameSpace {

    static DerbyTranslator instance;

    private static Thread mainThread;

    Driver driver;
    String databaseName;

    private String APPSchemaId;

    private static final String ALL_TABLES_SQL = "select TABLENAME from SYS.SYSTABLES where TABLETYPE in ('T','A','V') and SCHEMAID=? order by TABLENAME";
    private static final String TABLE_COUNT_SQL = "select count(*) from SYS.SYSTABLES where TABLETYPE in ('T','A','V') and SCHEMAID=? and TABLENAME=?";

    private DerbyTranslator(String databaseName) throws RemoteException {

        this.databaseName = databaseName;
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

        JinixFileDescriptor fd = JinixRuntime.getRuntime().getTranslatorFile();
        boolean interactiveMode = false;
        if (fd == null) {
            // Running standalone. use for debugging.
            System.out.println("DerbyTranslator: running in interactive mode");
            interactiveMode = true;
        }

        if (args.length == 0) {
            System.err.println("DerbyTranslator: database name argument required");
        }

        String databaseName = args[0];
        Path root = Paths.get(databaseName).toAbsolutePath();
        databaseName = root.toString();

        if (interactiveMode) {
            processInteractive(databaseName, args);
            return;
        }

        try {
            instance = new DerbyTranslator(databaseName);
            JinixRuntime.getRuntime().bindTranslator(instance);
        } catch (RemoteException e) {
            e.printStackTrace();
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
    public DirectoryFileData getFileAttributes(String s) throws NoSuchFileException, RemoteException {

        if (s.equals("/")) {
            DirectoryFileData dfd = new DirectoryFileData();
            dfd.name = s;
            dfd.length = 0;
            dfd.type = DirectoryFileData.FileType.DIRECTORY;
            dfd.lastModified = 0;
            return dfd;
        }

        if (!exists(s)) {
            return null;
        }

        DirectoryFileData dfd = new DirectoryFileData();
        dfd.name = s.substring(1);
        dfd.length = 0;
        dfd.type = DirectoryFileData.FileType.FILE;
        dfd.lastModified = 0;
        return dfd;
    }

    @Override
    public void setFileAttributes(String s, DirectoryFileData directoryFileData) throws NoSuchFileException, RemoteException {

    }

    @Override
    public boolean exists(String s) throws InvalidPathException, RemoteException {

        if (s.equals("/")) {
            return true;
        }

        if (s.substring(1).contains("/")) {
            return false;
        }

        try {
            Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
            try {
                PreparedStatement ps = conn.prepareStatement(TABLE_COUNT_SQL);
                ps.setString(1, APPSchemaId);
                ps.setString(2, s.substring(1));
                ResultSet rs = ps.executeQuery();
                while(rs.next()) {
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

    @Override
    public String[] list(String s) throws NotDirectoryException, RemoteException {
        try {
            Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
            try {
                List<String> tableNames = new ArrayList<String>();
                PreparedStatement ps = conn.prepareStatement(ALL_TABLES_SQL);
                ps.setString(1, APPSchemaId);
                ResultSet tables = ps.executeQuery();
                while(tables.next()) {
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

    @Override
    public DirectoryFileData[] listDirectoryFileData(String s) throws NotDirectoryException, RemoteException {
        try {
            Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
            try {
                List<DirectoryFileData> fileDataList = new LinkedList<DirectoryFileData>();
                PreparedStatement ps = conn.prepareStatement(ALL_TABLES_SQL);
                ps.setString(1, APPSchemaId);
                ResultSet tables = ps.executeQuery();
                while(tables.next()) {
                    DirectoryFileData dfd = new DirectoryFileData();
                    dfd.name = tables.getString(1);
                    dfd.length = 0;
                    dfd.type = DirectoryFileData.FileType.FILE;
                    dfd.lastModified = 0;
                    fileDataList.add(dfd);
                }
                ps.close();
                tables.close();

                return fileDataList.toArray(new DirectoryFileData[fileDataList.size()]);
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Translator Error", e);
        }
    }

    @Override
    public boolean createFileAtomically(String s) throws FileAlreadyExistsException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean createDirectory(String s) throws FileAlreadyExistsException, RemoteException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean createDirectories(String s) throws FileAlreadyExistsException, RemoteException {
        return false;
    }

    @Override
    public void delete(String s) throws NoSuchFileException, DirectoryNotEmptyException, RemoteException {

    }

    @Override
    public void copy(String s, String s1, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, UnsupportedOperationException, RemoteException {

    }

    @Override
    public void move(String s, String s1, CopyOption... copyOptions) throws NoSuchFileException, FileAlreadyExistsException, UnsupportedOperationException, RemoteException {

    }

    @Override
    public RemoteFileAccessor getRemoteFileAccessor(int i, String s, Set<? extends OpenOption> set) throws FileAlreadyExistsException, NoSuchFileException, RemoteException {

        if (!exists(s)) {
            throw new RuntimeException("Internal Error, file does not exist: "+s);
        }
        try {
            Connection conn = driver.connect("jdbc:derby:" + databaseName, null);
            return new TableRemoteFileAccessor(s.substring(1), conn);
        } catch (SQLException e) {
            throw new RemoteException("Translator Error", e);
        }
    }

    @Override
    public Object getKey(String s) throws RemoteException {
        return null;
    }

    @Override
    public List<FileAccessorStatistics> getOpenFiles(int i) throws RemoteException {
        return null;
    }
}
