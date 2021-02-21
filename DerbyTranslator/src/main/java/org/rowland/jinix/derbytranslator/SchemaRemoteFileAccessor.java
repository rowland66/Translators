package org.rowland.jinix.derbytranslator;

import org.rowland.jinix.naming.RemoteFileAccessor;
import org.rowland.jinix.naming.RemoteFileHandle;

import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;

public class SchemaRemoteFileAccessor implements RemoteFileAccessor {

    Connection conn;
    String on;

    SchemaRemoteFileAccessor(String objectName, Connection connection) {
        on = objectName;
        conn = connection;
    }

    @Override
    public RemoteFileHandle getRemoteFileHandle() throws RemoteException {
        return null;
    }

    @Override
    public byte[] read(int i, int i1) throws NonReadableChannelException, RemoteException {
        return new byte[0];
    }

    @Override
    public int write(int i, byte[] bytes) throws NonWritableChannelException, RemoteException {
        return 0;
    }

    @Override
    public long skip(long l) throws RemoteException {
        return 0;
    }

    @Override
    public int available() throws RemoteException {
        return 0;
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
    public void close() throws RemoteException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RemoteException("Translator error", e);
        }
    }

    @Override
    public void duplicate() throws RemoteException {

    }

    @Override
    public void force(boolean b) throws RemoteException {

    }

}
