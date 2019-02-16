package org.rowland.jinix.derbytranslator;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DerbyConnection extends UnicastRemoteObject implements RemoteConnection {

    private Connection conn;
    private int nextResultSetHandle = 1;
    Map<Integer, ResultSet> resultSetMap = new HashMap();

    DerbyConnection(Connection derbyConnection) throws RemoteException {
        conn = derbyConnection;
    }

    /**
     *
     * @param sql
     * @return a resultSet handle
     * @throws RemoteException
     */
    @Override
    public int executeQuery(String sql) throws SQLException, RemoteException {
        boolean throwSQLException = false;
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = null;
            try {
                rs = statement.executeQuery(sql);
            } catch (SQLException e) {
                throwSQLException = true;
                throw e;
            }
            int rsHandle = nextResultSetHandle++;
            resultSetMap.put(rsHandle, rs);
            return rsHandle;
        } catch (SQLException e) {
            if (throwSQLException) {
                throw e;
            }
            throw new RemoteException("Translator error", e);
        }
    }

    @Override
    public List<Object> resultSetNext(int resultSetHandle) throws RemoteException {
        ResultSet rs = resultSetMap.get(resultSetHandle);
        if (rs == null) {
            throw new RemoteException("Invalid resultSet handle: "+resultSetHandle);
        }
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            if (rs.next()) {
                int count = metaData.getColumnCount();
                List<Object> rtrnRow = new ArrayList(count);
                for (int i=1; i<=count; i++) {
                    switch (metaData.getColumnType(i))  {
                        case Types.CHAR:
                        case Types.VARCHAR:
                            rtrnRow.add(rs.getString(i));
                            break;
                        case Types.INTEGER:
                            rtrnRow.add(rs.getInt(i));
                            break;
                        case Types.DOUBLE:
                            rtrnRow.add(rs.getDouble(i));
                            break;
                        case Types.FLOAT:
                            rtrnRow.add(rs.getFloat(i));
                    }
                }
                return rtrnRow;
            }
            return null;
        } catch (SQLException e) {
            throw new RemoteException("Translator error", e);
        }
    }

    @Override
    public void resultSetClose(int resultSetHandle) throws RemoteException {
        ResultSet rs = resultSetMap.remove(resultSetHandle);
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new RemoteException("Translator error", e);
        }
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
    public ResultSetMetaDataDTO getResultSetMetaData(int resultSetHandle) throws RemoteException {
        ResultSet rs = resultSetMap.get(resultSetHandle);
        if (rs == null) {
            throw new RemoteException("Invalid resultSet handle: "+resultSetHandle);
        }
        try {
            return new ResultSetMetaDataDTO(rs.getMetaData());
        } catch (SQLException e) {
            throw new RemoteException("Translator error", e);
        }
    }
}
