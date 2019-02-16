package org.rowland.jinix.jext2;

import org.rowland.jinix.mbeans.FileSystemMBean;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import java.net.URI;
import java.rmi.RemoteException;

public class Jext2MBean extends StandardMBean implements FileSystemMBean {
    Jext2Translator t;

    Jext2MBean(Jext2Translator translator) throws NotCompliantMBeanException {
        super(FileSystemMBean.class);
        t = translator;
    }
    @Override
    public int getBlockSize() {
        return t.superblock.getBlocksize();
    }

    @Override
    public long getBlocksCount() {
        return t.superblock.getBlocksCount();
    }

    @Override
    public long getReservedBlocksCount() {
        return t.superblock.getReservedBlocksCount();
    }

    @Override
    public long getFreeBlocksCount() {
        return t.superblock.getFreeBlocksCount();
    }

    @Override
    public String getMountPath() {
        try {
            return t.getPathWithinParent();
        } catch (RemoteException e) {
            throw new RuntimeException("MBean failure");
        }
    }

    @Override
    public URI getURI() {
        return t.getURI();
    }
}
