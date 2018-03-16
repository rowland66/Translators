package org.rowland.jinix.translator;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TimeKeeper extends Remote {

    interface TimeWithOffset {
        public long getTime();
        public int getOffset();
    }

    public void setTimeOffset(int offset) throws RemoteException;

    public TimeWithOffset getTimeWithOffset() throws RemoteException;
}
