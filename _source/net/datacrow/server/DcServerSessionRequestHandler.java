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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.Security;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import com.cedarsoftware.util.io.JsonReader;
import com.cedarsoftware.util.io.JsonWriter;

import net.datacrow.core.DcConfig;
import net.datacrow.core.data.DcResultSet;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.DcSimpleValue;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.core.server.Connector;
import net.datacrow.core.server.requests.ClientRequest;
import net.datacrow.core.server.requests.ClientRequestApplicationSettings;
import net.datacrow.core.server.requests.ClientRequestExecuteSQL;
import net.datacrow.core.server.requests.ClientRequestItem;
import net.datacrow.core.server.requests.ClientRequestItemAction;
import net.datacrow.core.server.requests.ClientRequestItemKeys;
import net.datacrow.core.server.requests.ClientRequestItems;
import net.datacrow.core.server.requests.ClientRequestLogin;
import net.datacrow.core.server.requests.ClientRequestModules;
import net.datacrow.core.server.requests.ClientRequestReferencingItems;
import net.datacrow.core.server.requests.ClientRequestSimpleValues;
import net.datacrow.core.server.requests.ClientRequestUser;
import net.datacrow.core.server.requests.ClientRequestValueEnhancers;
import net.datacrow.core.server.response.DefaultServerResponse;
import net.datacrow.core.server.response.IServerResponse;
import net.datacrow.core.server.response.ServerActionResponse;
import net.datacrow.core.server.response.ServerApplicationSettingsRequestResponse;
import net.datacrow.core.server.response.ServerErrorResponse;
import net.datacrow.core.server.response.ServerItemKeysRequestResponse;
import net.datacrow.core.server.response.ServerItemRequestResponse;
import net.datacrow.core.server.response.ServerItemsRequestResponse;
import net.datacrow.core.server.response.ServerLoginResponse;
import net.datacrow.core.server.response.ServerModulesRequestResponse;
import net.datacrow.core.server.response.ServerSQLResponse;
import net.datacrow.core.server.response.ServerSimpleValuesResponse;
import net.datacrow.core.server.response.ServerValueEnhancersRequestResponse;
import net.datacrow.core.utilities.CompressedBlockInputStream;
import net.datacrow.core.utilities.CompressedBlockOutputStream;
import net.datacrow.server.security.SecurityCenter;

public class DcServerSessionRequestHandler extends Thread {
		
	private static Logger logger = Logger.getLogger(DcServerSessionRequestHandler.class);
	
	private Socket socket;
	private boolean canceled = false;
	private LocalServerConnector conn;
	private ClientRequest cr;
	
	private DcServerSession session;
	
	public DcServerSessionRequestHandler(DcServerSession session) {
		this.session = session;
	} 
	
	protected void cancel() {
		canceled = true;
	}
	
	protected boolean isCanceled() {
		return canceled;
	}
	
	@Override
    public void run() {
		if (isCanceled()) return;
        
		this.socket = session.getSocket();
		
		InputStream is = null;
		OutputStream os = null;
        
		try {
		    
	        Security.addProvider(new BouncyCastleProvider()); 
	        MessageDigest hash = MessageDigest.getInstance("SHA1");
	        is = new CompressedBlockInputStream(new DigestInputStream(socket.getInputStream(), hash));
	        os = new CompressedBlockOutputStream(new DigestOutputStream(socket.getOutputStream(), hash), 1024);
        	
        	JsonReader jr;
        	JsonReader.setUseUnsafe(true);

            // this is the connector we'll use on the server.
            // we'll use the actual logged on user credentials for the actions to be performed.
            conn = new LocalServerConnector();
            DcConfig.getInstance().setConnector(conn);
            
            while (!socket.isClosed()) {
                try {
                    jr = new JsonReader(is);
                    cr = (ClientRequest) jr.readObject();

                    if (!(cr instanceof ClientRequestLogin) && !(cr instanceof ClientRequestUser)) {
                        conn.setUser(session.getUser(cr));
                    }
                    
                    processRequest(os);
                } catch (IOException e) {
                    logger.info("Client session has been ended (" + socket.getInetAddress() + ")");
                    socket.close();
                } catch (ClassNotFoundException e) {
                    logger.error(e, e);
                    socket.close();
                }
            }
		} catch (Exception e) {
		    logger.error("Error while processing request " + cr + " for client " + (cr != null ? cr.getClientKey() : " null"), e);
		} finally {
        	try {
        		if (cr != null) cr.close();
        	} catch (Exception e) {
        	    logger.debug("An error occured while closing resources", e);
        	}
        }
    }
	
	/**
	 * Processes an request. The type of the request is checked before type casting.
	 * 
	 * @param cr
	 * @throws Exception
	 */
	private void processRequest(OutputStream os) throws Exception {
        JsonWriter jw = null;
        
        try {
        	IServerResponse sr = null;
	        switch (cr.getType()) {
	        case ClientRequest._REQUEST_ITEMS:
	        	sr = processItemsRequest((ClientRequestItems) cr);
	        	break;
	        case ClientRequest._REQUEST_ITEM:
	        	sr = processItemRequest((ClientRequestItem) cr);
	        	break;
	        case ClientRequest._REQUEST_ITEM_ACTION:
                sr = processItemActionRequest((ClientRequestItemAction) cr);
                break;
	        case ClientRequest._REQUEST_LOGIN:
	        	sr = processLoginRequest((ClientRequestLogin) cr);
	        	break;
	        case ClientRequest._REQUEST_ITEM_KEYS:
                sr = processItemKeysRequest((ClientRequestItemKeys) cr);
                break;
            case ClientRequest._REQUEST_EXECUTE_SQL:
                sr = processSQLRequest((ClientRequestExecuteSQL) cr);
                break;
            case ClientRequest._REQUEST_REFERENCING_ITEMS:
                sr = processReferencingItemsRequest((ClientRequestReferencingItems) cr);
                break;
            case ClientRequest._REQUEST_SIMPLE_VALUES:
                sr = processSimpleValuesRequest((ClientRequestSimpleValues) cr);
                break;
            case ClientRequest._REQUEST_MODULES:
                sr = processModulesRequest((ClientRequestModules) cr);
                break;
            case ClientRequest._REQUEST_APPLICATION_SETTINGS:
                sr = processApplicationSettingsRequest((ClientRequestApplicationSettings) cr);
                break;
            case ClientRequest._REQUEST_VALUE_ENHANCERS_SETTINGS:
                sr = processValueEnhancersRequest((ClientRequestValueEnhancers) cr);
                break;
            case ClientRequest._USER_MGT:
                sr = processUserManagementAction((ClientRequestUser) cr);
                break;                
                
            default:
                logger.error("No handler found for " + cr);
	        }
	        
	        if (sr != null) {
	            jw = new JsonWriter(os);  
	            jw.write(sr);
	            jw.flush();
		        
		        logger.debug("Send object to client");
	        } else {
	        	logger.error("Could not complete the request. The request type was unknown to the server. " + cr);
	        }
        } catch (IOException ioe) {
        	logger.error("Communication error between server and client", ioe);
        }	        
	}
	
    /** 
     * Retrieves items directly from the DataFilter.
     * 
     * @param cr
     * @throws Exception
     */
    private IServerResponse processUserManagementAction(ClientRequestUser cr) {
        if (cr.getActionType() == ClientRequestUser._ACTIONTYPE_CHANGEPASSWORD) {
            SecurityCenter.getInstance().changePassword(cr.getUser(), cr.getPassword());
        } else {
            logger.error("Client Request User action type not supported");
        }
        return new DefaultServerResponse();
    }
	
	/** 
	 * Retrieves items directly from the DataFilter.
	 * 
	 * @param cr
	 * @throws Exception
	 */
	private IServerResponse processItemsRequest(ClientRequestItems cr) {
    	List<DcObject> items = conn.getItems(cr.getDataFilter(), cr.getFields());
        ServerItemsRequestResponse sr = new ServerItemsRequestResponse(items);
	    return sr;
	}
	
   private IServerResponse processItemKeysRequest(ClientRequestItemKeys cr) {
        Map<String, Integer> items = conn.getKeys(cr.getDataFilter());
        ServerItemKeysRequestResponse sr = new ServerItemKeysRequestResponse(items);
        return sr;
    }
	
	private IServerResponse processLoginRequest(ClientRequestLogin lr) {
		SecuredUser su = conn.login(lr.getUsername(), lr.getPassword());
		return new ServerLoginResponse(su);
	}
	
	private IServerResponse processSQLRequest(ClientRequestExecuteSQL csr) throws Exception {
	    DcResultSet result = conn.executeSQL(csr.getSQL());
        return new ServerSQLResponse(result);
    }
	
    private IServerResponse processReferencingItemsRequest(ClientRequestReferencingItems crri) throws Exception {
        List<DcObject> values = conn.getReferencingItems(crri.getModuleIdx(), crri.getID());
        return new ServerItemsRequestResponse(values);
    }
	
    private IServerResponse processSimpleValuesRequest(ClientRequestSimpleValues crsv) throws Exception {
        List<DcSimpleValue> values = conn.getSimpleValues(crsv.getModule(), crsv.isIncludeIcons());
        return new ServerSimpleValuesResponse(values);
    }
    
    private IServerResponse processModulesRequest(ClientRequestModules crm) throws Exception {
        return new ServerModulesRequestResponse();
    }
    
    private IServerResponse processApplicationSettingsRequest(ClientRequestApplicationSettings cras) throws Exception {
        return new ServerApplicationSettingsRequestResponse();
    }
    
    private IServerResponse processValueEnhancersRequest(ClientRequestValueEnhancers cras) throws Exception {
        return new ServerValueEnhancersRequestResponse();
    }
	   
    private IServerResponse processItemActionRequest(ClientRequestItemAction cr) {
        DcObject dco = cr.getItem();
        
        DcConfig dcc = DcConfig.getInstance();
        Connector conn = dcc.getConnector();
        
        IServerResponse sr;
        boolean success = false;
        Throwable t = null;
        
        try {
	        if (cr.getAction() == ClientRequestItemAction._ACTION_DELETE) {
	            success = conn.deleteItem(dco);
	        } else if (cr.getAction() == ClientRequestItemAction._ACTION_SAVE) {
	            success = conn.saveItem(dco);
	        }
        } catch (Exception e) {
            logger.error("Error while executing Item Action", e);
            t = e;
        }
        
        if (!success) {
        	sr = new ServerErrorResponse(t, t.getMessage());
        } else {
        	sr = new ServerActionResponse(success);
        }
        
        return sr;
    }
	
	private IServerResponse processItemRequest(ClientRequestItem cr) {
		DcObject result = null;
		int[] fields = cr.getFields();
		Object value = cr.getValue();
		
		int moduleIdx = cr.getModule();
		
		DcConfig dcc = DcConfig.getInstance();
		Connector conn = dcc.getConnector();
		
		if (cr.getSearchType() == ClientRequestItem._SEARCHTYPE_BY_ID) {
			result = conn.getItem(moduleIdx, (String) value, fields);
		} else if (cr.getSearchType() == ClientRequestItem._SEARCHTYPE_BY_EXTERNAL_ID) {
			result = conn.getItemByExternalID(moduleIdx, cr.getExternalKeyType(), (String) value);	
		} else if (cr.getSearchType() == ClientRequestItem._SEARCHTYPE_BY_KEYWORD) {
			result = conn.getItemByKeyword(moduleIdx, (String) cr.getValue());	
		} else if (cr.getSearchType() == ClientRequestItem._SEARCHTYPE_BY_UNIQUE_FIELDS) {
			result = conn.getItemByUniqueFields((DcObject) cr.getValue());	
        } else if (cr.getSearchType() == ClientRequestItem._SEARCHTYPE_BY_DISPLAY_VALUE) {
            result = conn.getItemByDisplayValue(cr.getModule(), (String) cr.getValue());  
        }
		
        ServerItemRequestResponse sr = new ServerItemRequestResponse(result);
	    return sr;
	}
}
