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

package net.datacrow.server;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.datacrow.core.DcConfig;
import net.datacrow.core.DcRepository;
import net.datacrow.core.data.DataFilter;
import net.datacrow.core.data.DataFilterEntry;
import net.datacrow.core.data.DcIconCache;
import net.datacrow.core.data.DcResultSet;
import net.datacrow.core.data.Operator;
import net.datacrow.core.enhancers.IValueEnhancer;
import net.datacrow.core.enhancers.ValueEnhancers;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcField;
import net.datacrow.core.objects.DcMapping;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.DcSimpleValue;
import net.datacrow.core.objects.Loan;
import net.datacrow.core.objects.Picture;
import net.datacrow.core.objects.ValidationException;
import net.datacrow.core.objects.helpers.User;
import net.datacrow.core.resources.DcResources;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.core.security.SecurityException;
import net.datacrow.core.server.Connector;
import net.datacrow.core.server.response.ServerModulesRequestResponse;
import net.datacrow.core.wf.tasks.DcTask;
import net.datacrow.server.data.DataManager;
import net.datacrow.server.db.DatabaseManager;
import net.datacrow.server.security.SecurityCenter;
import net.datacrow.settings.DcSettings;

import org.apache.log4j.Logger;

/**
 * A direct connection connector. 
 * This is only used for running in single instance mode.
 */
public class LocalServerConnector extends Connector {
	
	private static Logger logger = Logger.getLogger(LocalServerConnector.class.getName());
	
	private SecuredUser su;
	
    @Override
    public void initialize() {
        ValueEnhancers.initialize();
    }
    
    @Override
    public void close() {
    	su = null;
    }
    
    public void setUser(SecuredUser su) {
    	this.su = su;
    }

    @Override
    public void changePassword(User user, String password) {
    	SecurityCenter.getInstance().changePassword(user, password);
    }
    
	@Override
    public DcResultSet executeSQL(String sql) {
        DcResultSet data = new DcResultSet();

    	try {
	    	if (sql.trim().startsWith("select") || sql.trim().startsWith("SELECT") || sql.trim().startsWith("Select")) {
	    	    ResultSet rs = DatabaseManager.getInstance().executeSQL(getUser(), sql);
	    	    data.fill(rs);
	    	} else {
	    		DatabaseManager.getInstance().execute(getUser(), sql);
	    	}
    	} catch (Exception e) {
    	    DcConfig.getInstance().getConnector().displayError(e.getMessage());
    		logger.error(e, e);
    	}
    	return data;
    }

    @Override
    public SecuredUser getUser() {
        return su;
    }

	@Override
    public void createUser(User user, String password) {
    	DatabaseManager.getInstance().createUser(user, password);
    }
    
    @Override
    public void updateUser(User user) {
    	DatabaseManager.getInstance().setPriviliges(user);
    }

	@Override
    public List<DcObject> getItems(int moduleIdx, int[] fields) {
    	DataFilter df = new DataFilter(moduleIdx);
        return DataManager.getInstance().getItems(getUser(), df, fields);
    }

    @Override
    public SecuredUser login(String username, String password) {
    	try {
    		su = SecurityCenter.getInstance().login(username, password);
    	} catch (SecurityException se) {
    		su = null;
    	}
    	
    	return su;
    }

    @Override
    public void deleteModule(int moduleIdx) {
    	try {
    		DcModules.get(moduleIdx).delete();
    	} catch (Exception e) {
    		logger.error("An error occurred while deleting module " + DcModules.get(moduleIdx), e);
    	}
    }
    
    @Override
    public List<DcSimpleValue> getSimpleValues(int module, boolean icons) {
        return DataManager.getInstance().getSimpleValues(getUser(), module, icons);
    }

    @Override
    public int getCount(int module, int field, Object value) {
        return DataManager.getInstance().getCount(getUser(), module, field, value);
    }

    @Override
    public List<DcObject> getReferencingItems(int moduleIdx, String ID) {
        return DataManager.getInstance().getReferencingItems(getUser(), moduleIdx, ID);
    }

    @Override
    public boolean checkUniqueness(DcObject dco, boolean exitingItem) {
        return DatabaseManager.getInstance().isUnique(dco, exitingItem);
    }
    
    @Override
    public boolean deleteItem(DcObject dco) throws ValidationException {
    	dco.beforeDelete();
    	boolean success = DatabaseManager.getInstance().delete(getUser(), dco, false);
    	dco.afterDelete();
    	return success;
    }

    @Override
    public boolean saveItem(DcObject dco) throws ValidationException {
    	dco.beforeSave();
    	
    	boolean success;
    	// do not queue the queries since we are already in thread mode on both sides
    	if (dco.isNew())
    		success = DatabaseManager.getInstance().insert(getUser(), dco, false);
    	else
    	    success = DatabaseManager.getInstance().update(getUser(), dco, false);
    	
    	dco.afterSave();
    	return success;
    }

    @Override
    public Collection<Picture> getPictures(String parentID) {
        DataFilter df = new DataFilter(DcModules._PICTURE);
        df.addEntry(new DataFilterEntry(DcModules._PICTURE, Picture._A_OBJECTID, Operator.EQUAL_TO, parentID));
		List<DcObject> items =  DataManager.getInstance().getItems(getUser(), df);
		
        Collection<Picture> pictures = new ArrayList<Picture>();
        for (DcObject dco : items) {
            pictures.add((Picture) dco);
        }
        
        return pictures;
    }

    @Override
    public Collection<DcObject> getReferences(int mappingModuleIdx, String parentKey, boolean full) {
		DataFilter df = new DataFilter(mappingModuleIdx);
		df.addEntry(new DataFilterEntry(mappingModuleIdx, DcMapping._A_PARENT_ID, Operator.EQUAL_TO, parentKey));
		int[] fields = full ? null : DcModules.get(mappingModuleIdx).getMinimalFields(null);
		return DataManager.getInstance().getItems(getUser(), df, fields);
    }

    @Override
    public Map<String, Integer> getChildrenKeys(String parentKey, int childModuleIdx) {
    	DataFilter df = new DataFilter(childModuleIdx);
        DcModule module = DcModules.get(childModuleIdx);
        df.addEntry(new DataFilterEntry(DataFilterEntry._AND, childModuleIdx, module.getParentReferenceFieldIndex(), Operator.EQUAL_TO, parentKey));
        return DataManager.getInstance().getKeys(getUser(), df);
    }

    @Override
    public Collection<DcObject> getChildren(String parentKey, int childModuleIdx, int[] fields) {
        DataFilter df = new DataFilter(childModuleIdx);
        DcModule module = DcModules.get(childModuleIdx);
        df.addEntry(new DataFilterEntry(DataFilterEntry._AND, childModuleIdx, module.getParentReferenceFieldIndex(), Operator.EQUAL_TO, parentKey));
        return DataManager.getInstance().getItems(getUser(), df, fields);
    }

    @Override
    public Loan getCurrentLoan(String parentKey) {
        DataFilter df = new DataFilter(DcModules._LOAN);
        df.addEntry(new DataFilterEntry(DcModules._LOAN, Loan._B_ENDDATE, Operator.IS_EMPTY, null));
        df.addEntry(new DataFilterEntry(DcModules._LOAN, Loan._D_OBJECTID, Operator.EQUAL_TO, parentKey));
        List<DcObject> items = DataManager.getInstance().getItems(getUser(), df);
        return items.size() > 0 ? (Loan) items.get(0) : new Loan();
    }

    @Override
    public List<DcObject> getLoans(String parentKey) {
        DataFilter df = new DataFilter(DcModules._LOAN);
        df.addEntry(new DataFilterEntry(DcModules._LOAN, Loan._D_OBJECTID, Operator.EQUAL_TO, parentKey));
        List<DcObject> items = DataManager.getInstance().getItems(getUser(), df);
        return items;
    }

    @Override
    public DcObject getItemByExternalID(int moduleIdx, String type, String externalID) {
        return DataManager.getInstance().getItemByExternalID(getUser(), moduleIdx, type, externalID);
    }
    
	@Override
	public DcObject getItemByDisplayValue(int moduleIdx, String displayValue) {
        return DataManager.getInstance().getItemByDisplayValue(getUser(), moduleIdx, displayValue);
	}

    @Override
    public DcObject getItemByKeyword(int moduleIdx, String keyword) {
        return DataManager.getInstance().getItemByKeyword(getUser(), moduleIdx, keyword);
    }

    @Override
    public DcObject getItem(int moduleIdx, String key) {
        return DataManager.getInstance().getItem(getUser(), moduleIdx, key, null);
    }

    @Override
    public DcObject getItem(int moduleIdx, String key, int[] fields) {
    	return DataManager.getInstance().getItem(getUser(), moduleIdx, key, fields);
    }

    @Override
    public Map<String, Integer> getKeys(DataFilter df) {
        return DataManager.getInstance().getKeys(getUser(), df);
    }

    @Override
    public List<DcObject> getItems(DataFilter df) {
    	return DataManager.getInstance().getItems(getUser(), df);
    }

    @Override
    public List<DcObject> getItems(DataFilter df, int[] fields) {
    	return DataManager.getInstance().getItems(getUser(), df, fields);
    }

	@Override
	public void executeTask(DcTask task) {
		Thread t = new Thread(task);
		
		try {
		    t.join();
		} catch (Exception e) {
		    logger.error(e, e);
		}
		
		t.start();
		
	}

	@Override
	public DcObject getItemByUniqueFields(DcObject dco) {
		return DataManager.getInstance().getItemByUniqueFields(getUser(), dco);
	}

	@Override
	public void dropUser(User user) {
		DatabaseManager.getInstance().deleteUser(user);
	}
	
    @Override
    public ServerModulesRequestResponse getModules() {
    	return null;
    }

    @Override
    public Map<DcField, Collection<IValueEnhancer>> getValueEnhancers() {
        return ValueEnhancers.getEnhancers();
    }

    @Override
    public void shutdown(boolean saveChanges) {
        DcConfig dcc = DcConfig.getInstance();
        dcc.getClientSettings().save();
        dcc.getConnector().close();
        
        logger.info(DcResources.getText("msgApplicationStops"));
        
        DcIconCache.getInstance().deleteIcons();
        DcSettings.set(DcRepository.Settings.stGracefulShutdown, Boolean.TRUE);
            
        DcSettings.save();
        DcModules.save();
        
        DcServer.getInstance().shutdown();
        
        DatabaseManager.getInstance().closeDatabases(false);
    }
}
