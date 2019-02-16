package org.rowland.jinix.derbytranslator;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class DataSourceStub implements DataSource, Serializable {

    RemoteDataSource ds;
    PrintWriter logWriter = null;

    DataSourceStub(RemoteDataSource remoteDataSource) {
        ds = remoteDataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            return new ConnectionStub(ds.getConnection(null, null));
        } catch (RemoteException e) {
            throw new SQLException("Internal error", e);
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        try {
            return new ConnectionStub(ds.getConnection(username, password));
        } catch (RemoteException e) {
            throw new SQLException("Internal error", e);
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
