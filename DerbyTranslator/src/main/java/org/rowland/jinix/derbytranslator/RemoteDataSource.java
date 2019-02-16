package org.rowland.jinix.derbytranslator;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteDataSource extends Remote {

    RemoteConnection getConnection(String username, String password) throws RemoteException;


}
