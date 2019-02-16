package org.rowland.jinix.derbytranslator;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetMetaDataDTO implements ResultSetMetaData, Serializable {

    private Data[] data;
    
    ResultSetMetaDataDTO(ResultSetMetaData metaData) {
        try {
            int colCount = metaData.getColumnCount();
            data = new Data[colCount];

            for (int i=1; i<=colCount; i++) {
                data[i-1].isAutoIncrement = metaData.isAutoIncrement(i);
                data[i-1].isCaseSensitive = metaData.isCaseSensitive(i);
                data[i-1].isSearchable = metaData.isSearchable(i);
                data[i-1].isCurrency = metaData.isCurrency(i);
                data[i-1].isNullable = metaData.isNullable(i);
                data[i-1].isSigned = metaData.isSigned(i);
                data[i-1].columnDisplaySize = metaData.getColumnDisplaySize(i);
                data[i-1].columnLabel = metaData.getColumnLabel(i);
                data[i-1].columnName = metaData.getCatalogName(i);
                data[i-1].schemaName = metaData.getSchemaName(i);
                data[i-1].precision = metaData.getPrecision(i);
                data[i-1].scale = metaData.getScale(i);
                data[i-1].tableName = metaData.getTableName(i);
                data[i-1].catalogName = metaData.getCatalogName(i);
                data[i-1].columnType = metaData.getColumnType(i);
                data[i-1].isReadOnly = metaData.isReadOnly(i);
                data[i-1].isWritable = metaData.isWritable(i);
                data[i-1].isDefinitelyWritable = metaData.isDefinitelyWritable(i);
                data[i-1].columnClassName = metaData.getColumnClassName(i);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Internal error", e);
        }
    }
    
    private static class Data implements Serializable {
        boolean isAutoIncrement;
        boolean isCaseSensitive;
        boolean isSearchable;
        boolean isCurrency;
        int isNullable;
        boolean isSigned;
        int columnDisplaySize;
        String columnLabel;
        String columnName;
        String schemaName;
        int precision;
        int scale;
        String tableName;
        String catalogName;
        int columnType;
        String columnTypeName;
        boolean isReadOnly;
        boolean isWritable;
        boolean isDefinitelyWritable;
        String columnClassName;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return data.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return data[column-1].isAutoIncrement;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return data[column-1].isCaseSensitive;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return data[column-1].isSearchable;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return data[column-1].isCurrency;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return data[column-1].isNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return data[column-1].isSigned;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return data[column-1].columnDisplaySize;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return data[column-1].columnLabel;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return data[column-1].columnName;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return data[column-1].schemaName;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return data[column-1].precision;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return data[column-1].scale;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return data[column-1].tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return data[column-1].catalogName;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return data[column-1].columnType;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return data[column-1].columnTypeName;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return data[column-1].isReadOnly;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return data[column-1].isWritable;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return data[column-1].isDefinitelyWritable;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return data[column-1].columnClassName;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
