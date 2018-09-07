/******************************************************************************

 *                                     __                                     *
 *                              <-----/@@\----->                              *
 *                             <-< <  \\//  > >->                             *
 *                               <-<-\ __ /->->                               *
 *                               Data /  \ Crow                               *
 *                                   ^    ^                                   *
 *                              info@datacrow.net                             *
 *                                                                            *
 *                       This file is part of Data Crow.                      *
 *       Data Crow is free software; you can redistribute it and/or           *
 *        modify it under the terms of the GNU General Public                 *
 *       License as published by the Free Software Foundation; either         *
 *              version 3 of the License, or any later version.               *
 *                                                                            *
 *        Data Crow is distributed in the hope that it will be useful,        *
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *           MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.             *
 *           See the GNU General Public License for more details.             *
 *                                                                            *
 *        You should have received a copy of the GNU General Public           *
 *  License along with this program. If not, see http://www.gnu.org/licenses  *
 *                                                                            *
 ******************************************************************************/

package net.datacrow.server.db;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.datacrow.core.DcConfig;
import net.datacrow.core.DcRepository;
import net.datacrow.core.Version;
import net.datacrow.core.data.DataFilter;
import net.datacrow.core.data.DataFilterConverter;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.helpers.User;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.core.server.Connector;
import net.datacrow.core.utilities.CoreUtilities;
import net.datacrow.server.data.DataManager;
import net.datacrow.server.security.SecurityCenter;
import net.datacrow.server.upgrade.SystemUpgradeException;
import net.datacrow.settings.DcSettings;
import net.datacrow.settings.definitions.DcFieldDefinition;

import org.apache.log4j.Logger;
import org.hsqldb.error.ErrorCode;

/**
 * The database manager is responsible for all databases.
 * This class is the only service providing access to the databases.
 * 
 * @author Robert Jan van der Waals
 */
public class DatabaseManager {

    private static Logger logger = Logger.getLogger(DatabaseManager.class.getName());
    
    private static DatabaseManager instance = new DatabaseManager();
    
    private DcDatabase db = new DcDatabase();

    private boolean initialized = false;
    
    private ConcurrentHashMap<String, Connection> connections = new ConcurrentHashMap<String, Connection>();
    
    private Connection adminConnection;
    private Connection checkConnection;
   
    /**
     * Retrieves the sole instance of this class
     */
    public static synchronized DatabaseManager getInstance() {
        return instance;
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    private DatabaseManager() {}
    
    /**
     * Initializes the database. A connection with the HSQL database engine is established
     * and the if needed the databases are upgraded.
     */
    public void initialize() {
        
        try {
            long start = logger.isDebugEnabled() ? new Date().getTime() : 0;

            db.initiliaze();
            
            if (logger.isDebugEnabled()) {
                long end = new Date().getTime();
                logger.debug("Initialization of the database (DcDatabase) took " + (end - start) + "ms");
            }  
            
            start = logger.isDebugEnabled() ? new Date().getTime() : 0;
            
            db.getConversions().load();
            db.getConversions().execute();

            if (logger.isDebugEnabled()) {
                long end = new Date().getTime();
                logger.debug("Execution of the database conversion scripts took " + (end - start) + "ms");
            }  

            start = logger.isDebugEnabled() ? new Date().getTime() : 0;
            
            db.cleanup();
            
            if (logger.isDebugEnabled()) {
                long end = new Date().getTime();
                logger.debug("Database cleanup took " + (end - start) + "ms");
            }  
            
            checkConnection = getConnection("DC_ADMIN", "UK*SOCCER*96");
            initialized = true;

        } catch (SystemUpgradeException sue) {
            logger.fatal("The upgrade of the database has failed.", sue);
            Connector conn = DcConfig.getInstance().getConnector();
            conn.notifyDatabaseFailure(sue.getMessage());
        } catch (DatabaseVersionException dve) {
            logger.fatal("The version of the database could not be set. This will result in "
            		+ "a upgrade loop.", dve);
            Connector conn = DcConfig.getInstance().getConnector();
            conn.notifyDatabaseFailure(dve.getMessage());
        } catch (DatabaseInvalidException die) {
            logger.fatal("The current database is invalid.", die);
            Connector conn = DcConfig.getInstance().getConnector();
            conn.notifyDatabaseFailure(die.getMessage());
        } catch (DatabaseInitializationException die) {
            logger.fatal("The current database could not be initialized.", die);
            Connector conn = DcConfig.getInstance().getConnector();
            conn.notifyDatabaseFailure(die.getMessage());
        }
    }

    /**
     * Retrieves the original database version (the version before the database was upgraded). 
     * @return
     */
    public Version getOriginalVersion() {
        return db.getOriginalVersion();
    }
    
    /**
     * Retrieves the current database version.
     * @return The current version. If version information could not be found an undetermined
     * version is returned.
     */
    public Version getVersion() {
        Connection c = getAdminConnection();
        return c != null ? db.getVersion(c) : new Version(0,0,0,0);
    }
    
    /**
     * Retrieves the count of currently queued queries. 
     */
    public int getQueueSize() {
    	return db.getQueueSize();
    }
    
    /**
     * Apply settings on the databases. 
     */
    public void applySettings() {
        db.setDbProperies(getAdminConnection());
    }
    
    /**
     * Checks whether the database is available. It could be the database is locked.
     */
    public boolean isLocked() {
        return getAdminConnection() == null;
    }
    
    /**
     * Closes the database connections.
     * @param compact Indicates if the database should be compacted.
     */
    public void closeDatabases(boolean compact) {
        try {
            if (db != null) {
                
                // calculates the conversions based on the alter module wizard
                Conversions conversions = db.getConversions();
                conversions.calculate();
                conversions.save();
                
                Connection c = getAdminConnection();
                
                try {
                	if (checkConnection != null)
                		checkConnection.close();
                } catch (SQLException se) {
                    logger.error(se, se);
                }
                
                if (c != null) {
                    Statement stmt = c.createStatement();

                    if (compact)
                        stmt.execute("SHUTDOWN COMPACT");
                    else 
                        stmt.execute("SHUTDOWN");
                    
                    stmt.close();
                    c.close();
                }
                
                // just to make sure the database is really released..
                org.hsqldb.DatabaseManager.closeDatabases(0);
            }
        } catch (Exception exp) {
            logger.error("Error while closing the database (compact = " + compact + ")", exp);
        }
    }

    private boolean isClosed(Connection c) {
        boolean closed = false;
        try {
            closed = c == null || c.isClosed();
            
            if (c != null) {
            	Statement stmt = c.createStatement();
            	stmt.execute("select * from version");
            	stmt.close();
            }
        } catch (SQLException se) {
            closed = true;
        }
        return closed;
    }
    
    /**
     * Returns a new connection to the database based on the logged on user.
     */
	public Connection getConnection(SecuredUser su) {
    	
    	Connection connection = connections.get(su.getUser().getID());
    	
        if (isClosed(connection)) {
        	
        	// for good measure
        	try {
        		if (connection != null) 
        			connection.close();
        	} catch (Exception e) {
        		logger.debug(e, e);
        	}
        	
            connection = getConnection(su.getUsername(), su.getPassword());
            logger.debug("Created a new, normal, database connection");
            
            try {
            	connections.put(su.getUser().getID(), connection);
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
        
        return connection;
    }
	
	public void doDatabaseHealthCheck() throws DatabaseInvalidException {
        try {
            String name = db.getName();
            
            if (new File(DcConfig.getInstance().getDatabaseDir(), db.getName() + ".script").exists()) {
	            Class.forName(DcSettings.getString(DcRepository.Settings.stDatabaseDriver));
	            String address = "jdbc:hsqldb:file:" + DcConfig.getInstance().getDatabaseDir() + name;
	            Connection c = DriverManager.getConnection(address, "SA", null);
	            
	            Version v = getVersion();
	            
	            if (v.isOlder(new Version(3, 12, 5, 0)))
	            	throw new DatabaseInvalidException("The version of the database [" + v.toString() + "] " +
	            			"is not supported by this version of Data Crow. First upgrade to version 3.12.5. " +
	            			"After upgrading to version 3.12.5 you can use the database with this version of " +
	            			"Data Crow. NOTE: It is always important to make backups; before and after upgrading " +
	            			"to version 3.12.5 make a backup of Data Crow (Tools > Backup & Restore).");
	            
	            c.close();
            }
        } catch (SQLException e) {
            int errorCode = e.getErrorCode();
        	if (errorCode == ErrorCode.LOCK_FILE_ACQUISITION_FAILURE ||
        	    errorCode == ErrorCode.LOCK_FILE_ACQUISITION_FAILURE * -1) {
        		throw new DatabaseInvalidException("The database is locked as it is being used by another instance"
        				+ " of Data Crow. Close the other applications using this database and restart Data Crow.");
        	} else if (
        	     errorCode != ErrorCode.ACCESS_IS_DENIED &&
        	     errorCode != ErrorCode.ACCESS_IS_DENIED * -1 &&
        	    !(e instanceof SQLInvalidAuthorizationSpecException)) {
        	    
        		logger.fatal("Database health check failed.", e);
        		
        		try {
	        		CoreUtilities.rename(
	        				new File(DcConfig.getInstance().getDatabaseDir(), db.getName() + ".script"), 
	        				new File(DcConfig.getInstance().getDatabaseDir(), db.getName() + "_error.script"), true);
	        		
	        		CoreUtilities.rename(
	        				new File(DcConfig.getInstance().getDatabaseDir(), db.getName() + ".properties"), 
	        				new File(DcConfig.getInstance().getDatabaseDir(), db.getName() + "_error.properties"), true);
	        		new File(DcConfig.getInstance().getDatabaseDir(), db.getName() + ".lck").delete();
	        		
        		} catch (Exception e2) {
        			logger.error("Could not rename the database", e2);
        		}
        		
        		throw new DatabaseInvalidException("The database contains unrecoverable errors. " + 
        				"The database has been renamed to " + db.getName() + "_error. Start Data Crow again " +
        				"and restore your latest backup. Review the log file (data_crow.log) for the details.", e);
        	}
        } catch (ClassNotFoundException cnfe) {
        	throw new DatabaseInvalidException("The database driver could not be found", cnfe);
        }
	}
    
    /**
     * Returns a connection for the given user credentials.
     * Note that this connection always needs to be closed manually.
     * For re-use use {@link #getConnection()} or {@link #getAdminConnection()}.
     * 
     * @param username
     * @param password
     * @return
     */
    public Connection getConnection(String username, String password) {
        String address = null;
        
        try {
            String name = db.getName();
            Class.forName(DcSettings.getString(DcRepository.Settings.stDatabaseDriver));
            address = "jdbc:hsqldb:file:" + DcConfig.getInstance().getDatabaseDir() + name;
            Connection c = DriverManager.getConnection(address, username.toUpperCase(), password);
            c.setAutoCommit(true);
            return c;
            
        } catch (SQLException e) {
        	if (e.getErrorCode() == ErrorCode.ACCESS_IS_DENIED ||
        	    e instanceof SQLInvalidAuthorizationSpecException) {
        		
                logger.debug("User [" + username + "] cannot login, username/password incorrect");
        	} else {
        		logger.error("Error while logging in with user [" + username + "]", e);
        	}
        } catch (ClassNotFoundException cnfe) {
        	logger.error("The database driver could not be found", cnfe);
        }
        
        return null;
    }
    
    public Map<String, Integer> getKeys(SecuredUser su, DataFilter df) {
    	Map<String, Integer> data = new LinkedHashMap<String, Integer>();

        try {
        	DataFilterConverter dfc = new DataFilterConverter(df);
        	String sql = dfc.toSQL(new int[] {DcObject._ID}, true, false);
        	
        	if (logger.isDebugEnabled())
        		logger.debug(sql);
        	
            ResultSet rs = executeSQL(su, sql);
            
            int moduleIdx;
            while (rs.next()) {
            	try {
            		moduleIdx = rs.getInt("MODULEIDX");
            	} catch (Exception e) {
            		moduleIdx = df.getModule();
            	}
            	data.put(rs.getString("ID"), moduleIdx);
            }
            
            rs.close();
        } catch (SQLException e) {
            if (!e.getMessage().equals("No ResultSet was produced"))
                logger.error("Error while executing query", e);
        }
        return data;
    }

    /**
     * Executes a query. 
     * @param sql SQL statement.
     * @param log Indicates if information on the query should be logged.
     * @return The result set.
     */
    public ResultSet executeSQL(SecuredUser su, String sql) throws SQLException {
        Connection c = getConnection(su);
        Statement stmt = c.createStatement();
        return stmt.executeQuery(sql);
    }
    
    /**
     * Executes a query. 
     * @param sql SQL statement.
     * @param log Indicates if information on the query should be logged.
     */
    public boolean execute(SecuredUser su, String sql) throws SQLException {
        Statement stmt = null;
        boolean success = false;
        
        try {
            Connection c = getConnection(su);
            stmt = c.createStatement();
            success = stmt.execute(sql);
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (Exception e) {
                logger.debug("Failed to close statement", e);
            }
        }
        
        return success;
    }

    /**
     * Executes a query. 
     * @param sql SQL statement.
     * @param log Indicates if information on the query should be logged.
     * @return The result set.
     */
    public ResultSet executeQueryAsAdmin(String sql) throws SQLException {
        Connection c = getAdminConnection();
        Statement stmt = c.createStatement(); // can't close the Statement since this would close the rs as well.
        return stmt.executeQuery(sql);
    }
    
    /**
     * Executes a query. 
     * @param sql SQL statement.
     * @param log Indicates if information on the query should be logged.
     * @return The result set.
     */
    public void executeAsAdmin(String sql) throws SQLException {
        Connection c = getAdminConnection();
        Statement stmt = null;
        
        try {
            stmt = c.createStatement();
            stmt.execute(sql);
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (Exception e) {
                logger.debug("Failed to close statement", e);
            }
        }
    }
    
    /**
     * Update the item in the database with the values from the specified item.
     * @param dco
     */
    public boolean update(SecuredUser su, DcObject dco, boolean queued) {
        boolean success = false;
        if (dco.isChanged()) {
            UpdateQuery query = new UpdateQuery(su, dco);
            if (queued) {
                db.queue(query);
                success = true;
            } else {
                query.run();
                success = query.isSuccess();
            }
        }
        return success;
    }

    /**
     * Stores the item in the database.
     * @param dco
     */
    public boolean insert(SecuredUser su, DcObject dco, boolean queued) {
        Query query = new InsertQuery(su, dco);
        
        boolean success = false;
        if (queued) {
            db.queue(query);
            success = true;
        } else {
            query.run();
            success = query.isSuccess();
        }
        
        return success;
    }

    public boolean delete(SecuredUser su, DcObject dco, boolean queued) {
        Query query = new DeleteQuery(su, dco);
        
        boolean success = false;
        if (queued) {
            db.queue(query);
            success = true;
        } else {
            query.run();
            success = query.isSuccess();
        }
        
        return success;
    }

    /**
     * Checks the database to see if the item already exists.
     * @param o The item to check.
     * @param isExisting Indicates if the check is performed for a new or an existing item.
     */
    public boolean isUnique(DcObject o, boolean isExisting) {
        if (o.hasPrimaryKey() && !o.getModule().isChildModule()) {
            boolean hasUniqueFields = false;
            DcObject dco = o.getModule().getItem();

            for (DcFieldDefinition def : o.getModule().getFieldDefinitions().getDefinitions()) {
                if (def.isUnique()) {
                    dco.setValue(def.getIndex(), o.getValue(def.getIndex()));
                    hasUniqueFields = true;
                }
            }
                
            if (hasUniqueFields) {
                DataFilter df = new DataFilter(dco);
                Map<String, Integer> keys = DataManager.getInstance().getKeys(
                		SecurityCenter.getInstance().getAdmin(), df);
                
                int count = 0;
                for (String key : keys.keySet())
                	count = !isExisting || !key.equals(o.getID()) ? count + 1 : count;

                if (count > 0) return false;
            }
        }
        return true;
    }
    
    /*****************************************************************************************
     * Security methods
     *****************************************************************************************/

    /**
     * Creates an admin connection to the database.
     */
    public synchronized Connection getAdminConnection() {
        if (isClosed(adminConnection)) {
            adminConnection = getConnection("DC_ADMIN", "UK*SOCCER*96");
            logger.debug("Created a new, admin, database connection");
        }
        
        // Do not store this connection.
        // This is only needed when the default admin user has not been created
        if (adminConnection == null)
        	return getConnection("SA", "");

        return adminConnection;
    }
    
    /**
     * Change the password for the given user.
     * @param user
     * @param password
     */
    public void changePassword(User user, String password) {
        Connection c = null;
        Statement stmt = null;
        
        try {
            c = getAdminConnection();
            stmt = c.createStatement();

            String sql = "ALTER USER " + user.getValue(User._A_LOGINNAME) + " SET PASSWORD '" + password + "'";
            stmt.execute(sql);
            
        } catch (SQLException se) {
            logger.error(se, se);
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (c != null) c.close();
            } catch (Exception e) {
                logger.debug("An error occured while closing resources", e);
            }
        }
    }
    
    /**
     * Removes a user from the database
     */
    public void deleteUser(User user) {
        Connection c = null;
        Statement stmt = null;
        
        try {
            c = getAdminConnection();
            stmt = c.createStatement();

            user.setNew(false);
            user.load(null);
            String sql = "DROP USER " + user.getValue(User._A_LOGINNAME);
            stmt.execute(sql);
            
            SecurityCenter.getInstance().logoff(user);
            
        } catch (SQLException se) {
            logger.error(se, se);
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (c != null) c.close();
            } catch (Exception e) {
                logger.debug("Failed to close database resources", e);
            }
        }
    }
    
    /**
     * Creates a user with all the correct privileges
     */
    public void createUser(User user, String password) {
        Connection c = null;
        Statement stmt = null;
        
        try {
            c = getAdminConnection();
            stmt = c.createStatement();

            String sql = "CREATE USER " + user.getDisplayString(User._A_LOGINNAME).toUpperCase() + " PASSWORD '" + password + "'";
            if (user.isAdmin()) 
                sql += " ADMIN";
            
            stmt.execute(sql);

            setPriviliges(user);
            
        } catch (SQLException se) {
            logger.error(se, se);
        } finally {
            try {
                if (stmt != null) stmt.close();
                if (c != null) c.close();
            } catch (Exception e) {
                logger.debug("Failed to close database reources", e);
            }
        }
    }    
    
    /**
     * Updates the privileges of an existing user.
     * @param user
     */
    public void setPriviliges(User user) {
        
        long start = logger.isDebugEnabled() ? new Date().getTime() : 0;
        
        for (DcModule module : DcModules.getAllModules())
            setPriviliges(module, user);
        
        if (logger.isDebugEnabled()) {
            long end = new Date().getTime();
            logger.debug("Setting the correct database privileges " + (end - start) + "ms");
        }  
    }
    
    /**
     * Updates the privileges of an existing user. 
     * @param user
     * @param admin Indicates if the user is an administrator.
     */
    public void setPriviliges(String user, boolean admin) {
        for (DcModule module : DcModules.getAllModules())
            setPriviliges(module, user, admin);
    }
    
    protected void setPriviliges(DcModule module, User user) {
        if (user == null) return;
        
        setPriviliges(module, (String) user.getValue(User._A_LOGINNAME), user.isAdmin());
    } 
        
    /**
     * Applies the users privileges on the database tables and columns.
     * @param module
     * @param user
     * @param admin
     */
    protected void setPriviliges(DcModule module, String user, boolean admin) {

       Connection c = null;
       Statement stmt = null;
       
       try {
            String tablename = module.getTableName();
            
            if (tablename == null || tablename.trim().length() == 0)
                return;
            
            c = getAdminConnection();
            stmt = c.createStatement();

            // check if the table exists
            boolean created = false;
            try {
                String sql = "SELECT TOP 1 * from " + tablename;
                stmt.execute(sql);
                created = true;
            } catch (SQLException se) {
                logger.debug("Table " + tablename + " has not yet been created, will not set priviliges");
            }
            
            if (created) {
                String sql = "REVOKE ALL PRIVILEGES ON TABLE " + tablename + " FROM " + user + " RESTRICT";
                stmt.execute(sql);
                
                if (admin) {
                    sql = "GRANT ALL ON TABLE " + tablename + " TO " + user;
                    stmt.execute(sql);
    
                } else {
                    sql = "GRANT SELECT ON TABLE " + tablename + " TO " + user;
                    stmt.execute(sql);
                    
                    if (module.isEditingAllowed()) {
                        sql = "GRANT UPDATE ON TABLE " + tablename + " TO " + user;
                        stmt.execute(sql);
                        sql = "GRANT INSERT ON TABLE " + tablename + " TO " + user;
                        stmt.execute(sql);
                    }
                    
                    if (admin || module.getIndex() == DcModules._PICTURE || module.getType() == DcModule._TYPE_MAPPING_MODULE) {
                        sql = "GRANT DELETE ON TABLE " + tablename + " TO " + user;
                        stmt.execute(sql);
                    }
                }
            }
        } catch (SQLException se) {
            logger.error(se, se);
        } finally {
            try {
                if (stmt != null) stmt.close();
            } catch (Exception e) {
                logger.debug("Failed to release database resources", e);
            }
        }
    }    
}

