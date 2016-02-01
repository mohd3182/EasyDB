/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.eg.oneware.easydb.connections;

/**
 *
 * @author mohamed.abdelalim
 */
public interface EasyConnection {

    public abstract String getServerName();

    public abstract void setServerName(String serverName);

    public abstract String getPortNumber();

    public abstract void setPortNumber(String portNumber);

    public abstract String getServiceName();

    public abstract void setServiceName(String serviceName);

    public abstract String getUsername();

    public abstract void setUsername(String username);

    public abstract String getPassword();

    public abstract void setPassword(String password);

    public abstract void setDBType(EasySupportDB easySupportDB);
}
