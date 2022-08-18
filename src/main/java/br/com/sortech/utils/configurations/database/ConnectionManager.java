package br.com.sortech.utils.configurations.database;

import java.sql.Connection;
import java.sql.DriverManager;


public class ConnectionManager {


    private String _url;
    private String _user;
    private String _password;

    public ConnectionManager(String driver, String url, String user, String password) throws Exception {

        try {
            Class.forName(driver).newInstance();
        } catch (Exception e) {
            throw e;
        }

        this.setUrl(url);
        this.setUser(user);
        this.setPassword(password);

    }




    public Connection getConnection() throws Exception {
        return (Connection) DriverManager.getConnection(getUrl(), getUser(), getPassword());
    }


    public void setUrl (String url) {
        this._url = url;
    }

    public String getUrl() {
        return this._url;
    }

    public void setUser (String user) {
        this._user = user;
    }

    public String getUser() {
        return this._user;
    }

    public void setPassword(String password) {
        this._password = password;
    }

    public String getPassword() {
        return this._password;
    }
}
