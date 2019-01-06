package org.rowland.jinix.derbytranslator;

import org.rowland.jinix.naming.RemoteFileAccessor;

import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.*;

public class TableRemoteFileAccessor extends UnicastRemoteObject implements RemoteFileAccessor {

    private String tableName;
    private Connection conn;
    private ResultSet rs;
    private ResultSetMetaData metaData;
    private byte[] currentRowByte;
    private int currentRowPosition;
    private long filePosition;
    private int openCount;


    TableRemoteFileAccessor(String tableName, Connection connection) throws RemoteException {
        try {
            this.tableName = tableName;
            this.conn = connection;
            PreparedStatement ps = conn.prepareStatement("select * from " + tableName);
            rs = ps.executeQuery();
            metaData = rs.getMetaData();
            loadDatabaseRowAsBytes();
            filePosition = 0;
            openCount = 1;
        } catch (SQLException e) {
            throw new RemoteException("Translator Error", e);
        }
    }

    @Override
    public synchronized byte[] read(int pgid, int length) throws NonReadableChannelException, RemoteException {
        try {
            byte[] rtrn = new byte[length];
            int rtrnPos = 0;
            while(rtrnPos < length && currentRowByte.length > 0) {
                if (currentRowPosition == currentRowByte.length) {
                    loadDatabaseRowAsBytes();
                    continue;
                }
                int bytesToCopy = Math.min(length-rtrnPos,currentRowByte.length-currentRowPosition);
                System.arraycopy(currentRowByte, currentRowPosition, rtrn, rtrnPos, bytesToCopy);
                rtrnPos += bytesToCopy;
                currentRowPosition += bytesToCopy;
                filePosition += bytesToCopy;
            }
            if (rtrnPos < length) {
                byte[] rtrn2 = new byte[rtrnPos];
                System.arraycopy(rtrn, 0, rtrn2, 0, rtrnPos);
                return rtrn2;
            }
            return rtrn;
        } catch (SQLException e) {
            throw new RemoteException("Translator Error", e);
        }
    }

    @Override
    public int write(int i, byte[] bytes) throws NonWritableChannelException, RemoteException {
        throw new NonWritableChannelException();
    }

    @Override
    public long skip(long l) throws RemoteException {
        return 0;
    }

    @Override
    public int available() throws RemoteException {
        return currentRowByte.length - currentRowPosition;
    }

    @Override
    public long getFilePointer() throws RemoteException {
        return this.filePosition;
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
    public void close() throws RemoteException {
        try {
            if (openCount > 0) {
                openCount--;
                if (openCount == 0) {
                    conn.close();
                }
            }
        } catch (SQLException e) {
            throw new RemoteException("Tranlator Error", e);
        }
    }

    @Override
    public void duplicate() throws RemoteException {
        openCount++;
    }

    @Override
    public void force(boolean b) throws RemoteException {

    }

    @Override
    public void flush() throws RemoteException {

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    private void loadDatabaseRowAsBytes() throws SQLException {
        StringBuilder currentRowString = new StringBuilder(1024);
        if (rs.next()) {
            int colCount = metaData.getColumnCount();
            for(int col=1; col<=colCount; col++) {
                if (col > 1) {
                    currentRowString.append(',');
                }
                if (isStringDataType(metaData.getColumnType(col))) {
                    currentRowString.append("\"" + rs.getString(col) + "\"");
                } else {
                    currentRowString.append(rs.getString(col));
                }
            }
            currentRowString.append('\n');
            currentRowByte = currentRowString.toString().getBytes(Charset.forName("US-ASCII"));
        } else {
            currentRowByte = new byte[0];
        }

        currentRowPosition = 0;
    }

    private boolean isStringDataType(int dataType) {
        return (dataType == Types.CHAR ||
                dataType == Types.LONGNVARCHAR ||
                dataType == Types.NCHAR ||
                dataType == Types.NVARCHAR ||
                dataType == Types.VARCHAR ||
                dataType == Types.LONGVARCHAR);
    }
}
