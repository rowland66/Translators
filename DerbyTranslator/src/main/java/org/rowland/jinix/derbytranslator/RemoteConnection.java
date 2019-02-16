package org.rowland.jinix.derbytranslator;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface RemoteConnection extends Remote {

    int executeQuery(String sql) throws SQLException, RemoteException;

    List<Object> resultSetNext(int resultSetHandle) throws RemoteException;

    void resultSetClose(int resultSetHandle) throws RemoteException;

    ResultSetMetaDataDTO getResultSetMetaData(int resultSetHandle) throws RemoteException;

    void close() throws RemoteException;
}
