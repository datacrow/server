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

package net.datacrow.server.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.datacrow.core.DcRepository;
import net.datacrow.core.data.DataFilter;
import net.datacrow.core.data.DataFilterEntry;
import net.datacrow.core.data.Operator;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcAssociate;
import net.datacrow.core.objects.DcField;
import net.datacrow.core.objects.DcImageIcon;
import net.datacrow.core.objects.DcMapping;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.DcProperty;
import net.datacrow.core.objects.DcSimpleValue;
import net.datacrow.core.objects.helpers.ExternalReference;
import net.datacrow.core.objects.helpers.Media;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.core.utilities.CoreUtilities;
import net.datacrow.server.db.DatabaseManager;
import net.datacrow.server.db.SelectQuery;
import net.datacrow.settings.definitions.DcFieldDefinition;

import org.apache.log4j.Logger;

/**
 * @author Robert Jan van der Waals        
 */ 
public class DataManager {

    private static Logger logger = Logger.getLogger(DataManager.class.getName());
    
    private static DataManager instance = new DataManager();
    
    private DataManager() {}
    
    public static synchronized DataManager getInstance() {
    	return instance;
    }
    
    public int getCount(SecuredUser su, int module, int field, Object value) {
        int count = 0;
        
        ResultSet rs = null;
        PreparedStatement ps = null;
        
        try {
            DcModule m = DcModules.get(module);
            DcField f = field > 0 ? m.getField(field) : null;
            String sql;
            if (f == null) {
                sql = "select count(*) from " + m.getTableName();
            } else if (f.getValueType() != DcRepository.ValueTypes._DCOBJECTCOLLECTION) {
                sql = "select count(*) from " + m.getTableName() + " where " + m.getField(field).getDatabaseFieldName() + 
                      (value == null ? " IS NULL " : " = ?");
            } else {
                if (value != null) {
                	m = DcModules.get(DcModules.getMappingModIdx(module, f.getReferenceIdx(), field));
                	sql = "select count(*) from " + m.getTableName() + " where " + m.getField(DcMapping._B_REFERENCED_ID).getDatabaseFieldName() + " = ?";
                } else { 
                	DcModule mapping = DcModules.get(DcModules.getMappingModIdx(module, f.getReferenceIdx(), field));
                	sql = "select count(*) from " + m.getTableName() + " MAINTABLE where not exists (select " + 
                	      mapping.getField(DcMapping._A_PARENT_ID).getDatabaseFieldName() + " from " + mapping.getTableName() + " where " +
                	      mapping.getField(DcMapping._A_PARENT_ID).getDatabaseFieldName() + " = MAINTABLE.ID)"; 
                }
            }
                
            ps = DatabaseManager.getInstance().getConnection(su).prepareStatement(sql);
            
            if (f != null && value != null) 
                ps.setObject(1, value instanceof DcObject ? ((DcObject) value).getID() : value);
            
            rs = ps.executeQuery();
            while (rs.next())
                count = rs.getInt(1);
            
        } catch (Exception e) {
            logger.error(e, e);
        } finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (SQLException se) {
                logger.debug("Could not close database resources", se);
            }
        }
        
        return count;
    }


    
    public List<DcObject> getReferencingItems(SecuredUser su, int moduleIdx, String ID) {
        List<DcObject> items = new ArrayList<DcObject>();
        
        DataFilter df;
        for (DcModule module : DcModules.getActualReferencingModules(moduleIdx)) {
            if ( module.getIndex() != moduleIdx && 
                 module.getType() != DcModule._TYPE_MAPPING_MODULE &&   
                 module.getType() != DcModule._TYPE_TEMPLATE_MODULE) {
                
                for (DcField field : module.getFields()) {
                    if (field.getReferenceIdx() == moduleIdx) {
                        df = new DataFilter(module.getIndex());
                        df.addEntry(new DataFilterEntry(DataFilterEntry._AND, module.getIndex(), field.getIndex(), Operator.EQUAL_TO, ID));
                        
                        for (DcObject dco : getItems(su, df, module.getMinimalFields(null))) {
                            if (!items.contains(dco))
                                items.add(dco);
                        }
                    }
                }
            }
        }  
        
        return items;
    }    
    
    public DcObject getItemByExternalID(SecuredUser su, int moduleIdx, String type, String externalID) {
        DcModule module =  DcModules.get(moduleIdx);
       
        if (module.getField(DcObject._SYS_EXTERNAL_REFERENCES) == null) return null;
        
        DcModule extRefModule =  DcModules.get(moduleIdx + DcModules._EXTERNALREFERENCE);
        String sql = "SELECT ID FROM " + extRefModule.getTableName() + " WHERE " +
            "UPPER(" + extRefModule.getField(ExternalReference._EXTERNAL_ID).getDatabaseFieldName() + ") = UPPER(?) AND " +
            "UPPER(" + extRefModule.getField(ExternalReference._EXTERNAL_ID_TYPE).getDatabaseFieldName() + ") = UPPER(?)";
        
        Connection conn = DatabaseManager.getInstance().getConnection(su);
        DcObject result = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, externalID);
            ps.setString(2, type);
            
            rs = ps.executeQuery();
            
            String referenceID;
            DcModule mappingMod;
            int idx;
            PreparedStatement ps2 = null;
            
            List<DcObject> items;
            while (rs.next()) {
                try {
                    referenceID = rs.getString(1);
                    
                    idx = DcModules.getMappingModIdx(extRefModule.getIndex() - DcModules._EXTERNALREFERENCE, extRefModule.getIndex(), DcObject._SYS_EXTERNAL_REFERENCES);
                    mappingMod = DcModules.get(idx);
                    sql = "SELECT * FROM " + DcModules.get(moduleIdx).getTableName() + " WHERE ID IN (" +
                    	  "SELECT OBJECTID FROM " + mappingMod.getTableName() + 
                          " WHERE " + mappingMod.getField(DcMapping._B_REFERENCED_ID).getDatabaseFieldName() + " = ?)";
        
                    ps2 = conn.prepareStatement(sql);
                    ps2.setString(1, referenceID);
                    
                    items = convert(ps2.executeQuery(), new int[] {DcObject._ID});
                    result = items.size() > 0 ? items.get(0) : null;
                    if (result != null) break;  
                } finally {
                    ps2.close();
                }             
            }
        } catch (SQLException se) {
            logger.error(se, se);
        } finally {
            try {
                if (ps != null) ps.close();
                if (rs != null) rs.close();
            } catch (Exception e) {
                logger.debug("Failed to release database resources", e);
            }
            
        }
        return result;
    }
    
    /**
     * Retrieves a matching item based on the 'isKey' setting.
     * @return Returns one of the matching item or NULL if none found
     */
    public DcObject getItemByUniqueFields(SecuredUser su, DcObject o) {
        DcObject result = null;
        
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
                Map<String, Integer> keys = getKeys(su, df);
                
                for (String key : keys.keySet()) {
                    result = o.isNew() || !key.equals(o.getID()) ? getItem(su, dco.getModule().getIndex(), key, null) : null;
                }
            }
        }
        return result;
    }

    public DcObject getItemByKeyword(SecuredUser su, int module, String keyword) {
        // Establish the names on which we will check if the item already exists.
        // Skip multiple checks for the external references; this will results in errors.
        String[] names = new String[(keyword.indexOf(" ") > -1 && keyword.indexOf(", ") == -1 && 
                DcModules.get(module).getType() != DcModule._TYPE_EXTERNALREFERENCE_MODULE ? 3 : 1)];
        names[0] = keyword;
        if (names.length > 1) {
            names[1] = keyword.replaceFirst(" ", ", ");
            names[2] = keyword.substring(keyword.indexOf(" ") + 1) + ", " + keyword.substring(0, keyword.indexOf(" "));
        }
        
        DcObject dco = null;
        for (String name : names) {
            dco = getItemByExternalID(su, module, DcRepository.ExternalReferences._PDCR, name);
            if (dco != null) break;
        }
        
        if (dco == null) {
            for (String name : names) {
                dco = getItemByDisplayValue(su, module, name);
                if (dco != null) break;
            }
        }
        
        return dco;
    }
    
    /**
     * Retrieves an item based on its display value.
     * @param module
     * @param s The display value.
     * @return Either the item or null. 
     */
    public DcObject getItemByDisplayValue(SecuredUser su, int moduleIdx, String s) {
        DcModule module = DcModules.get(moduleIdx);

        Collection<String> values = new ArrayList<String>();
        values.add(s);
        
        try {
        	String columns = module.getIndex() + " AS MODULEIDX";
        	for (DcField field : module.getFields()) {
        		if (!field.isUiOnly())
        			columns += "," + field.getDatabaseFieldName();
        	}
        	
            String query = "SELECT " + columns + " FROM " + module.getTableName() + " WHERE " + 
                "RTRIM(LTRIM(UPPER(" + module.getField(module.getSystemDisplayFieldIdx()).getDatabaseFieldName() + "))) =  UPPER(?)";
            
            if (module.getType() == DcModule._TYPE_ASSOCIATE_MODULE) {
            	query += " OR RTRIM(LTRIM(UPPER(" + module.getField(DcAssociate._A_NAME).getDatabaseFieldName() + "))) LIKE ?"; 
                String firstname = CoreUtilities.getFirstName(s);
                String lastname = CoreUtilities.getLastName(s);
                values.add("%" + CoreUtilities.getName(firstname, lastname) + "%");
            } 
            
            if (module.getType() == DcModule._TYPE_PROPERTY_MODULE) {
                query += " OR RTRIM(LTRIM(UPPER(" + module.getField(DcProperty._C_ALTERNATIVE_NAMES).getDatabaseFieldName() + "))) LIKE ?";
                values.add(";%" + s + "%;");
            }
                
            if (module.getType() == DcModule._TYPE_EXTERNALREFERENCE_MODULE) {
                // external references have a display value that consist of the type and the key.
                query += " OR (RTRIM(LTRIM(UPPER(" + module.getField(ExternalReference._EXTERNAL_ID_TYPE).getDatabaseFieldName() + "))) = ? " +
                		"      AND   RTRIM(LTRIM(UPPER(" + module.getField(ExternalReference._EXTERNAL_ID).getDatabaseFieldName() + "))) = ?)";
                values.add(s.indexOf(":") > -1 ?s.substring(0, s.indexOf(":")) : s);
                values.add(s.indexOf(":") > -1 ? s.substring(s.indexOf(":") + 2) : s);
            }
            
            PreparedStatement ps = DatabaseManager.getInstance().getConnection(su).prepareStatement(query);
            int idx = 1;
            for (String value : values)
                ps.setString(idx++, value.toUpperCase());
            
            List<DcObject> items = convert(ps.executeQuery(), new int[] {DcObject._ID});
            ps.close();
            
            return items.size() > 0 ? items.get(0) : null;
            
        } catch (SQLException e) {
            logger.error(e, e);
        }
        
        return null;
    }    
    
    /**
     * Retrieve the item based on its ID.
     * @param module
     * @param ID
     * @return null or the item if found.
     */
    public DcObject getItem(SecuredUser su, int module, String ID, int[] fields) {
        DataFilter df = new DataFilter(module);
        df.addEntry(new DataFilterEntry(module, DcObject._ID, Operator.EQUAL_TO, ID));
        List<DcObject> items = getItems(su, df, fields);
        DcObject item = items != null && items.size() > 0 ? items.get(0) : null;
        if (item != null) item.markAsUnchanged();
        return item;
    }    
    
    public Map<String, Integer> getKeys(SecuredUser su, DataFilter filter) {
        return DatabaseManager.getInstance().getKeys(su, filter); 
    }

    /**
     * Retrieve items using the specified data filter.
     * @see DataFilter
     * @param filter
     * @param fields 
     */
    public List<DcSimpleValue> getSimpleValues(SecuredUser su, int module, boolean icons) {
        DcModule m = DcModules.get(module);
        boolean useIcons = icons && m.getIconField() != null;
        String sql = "select ID, " + m.getField(m.getDisplayFieldIdx()).getDatabaseFieldName() + 
                      (useIcons ? ", " + m.getIconField().getDatabaseFieldName() : " ") +  
                     " from " + m.getTableName() +
                     " order by 2";

        List<DcSimpleValue> values = new ArrayList<DcSimpleValue>();
        
        ResultSet rs = null;
        try {
            rs = DatabaseManager.getInstance().executeSQL(su, sql);
            DcImageIcon icon; 
            DcSimpleValue sv;
            String s;
            while (rs.next()) {
                sv = new DcSimpleValue(rs.getString(1), rs.getString(2));
                if (useIcons) {
                    s = rs.getString(3);
                    if (!CoreUtilities.isEmpty(s)) {
                        icon = CoreUtilities.base64ToImage(s);
                        sv.setIcon(icon);
                    }
                }
                values.add(sv);
            }
                
        } catch (SQLException se) {
            logger.error(se, se);
        } finally {
            try {
                if (rs != null) rs.close();
            } catch (SQLException e) {}    
        }
        
        return values;
    }
    
    /**
     * Retrieve items using the specified data filter.
     * @see DataFilter
     * @param filter
     * @param fields 
     */
    public List<DcObject> getItems(SecuredUser su, DataFilter df, int[] fields) {
        return new SelectQuery(su, df, fields).run();
    }

    /** 
     * Overloaded 
     * @see #getItems(DataFilter, int[])
     */
    public List<DcObject> getItems(SecuredUser su, DataFilter filter) {
        return getItems(su, filter, null);
    }
    
    /**
     * Converts the result set to a collection of items.
	 * @param rs An unclosed SQL result set.
	 * @return Collection of items.
	 */
	public List<DcObject> convert(ResultSet rs, int[] requestedFields) {
		List<DcObject> objects = new ArrayList<DcObject>();

		try {
			rs.isLast();
		} catch (Exception exp) {
			return objects;
		}

		try {
			ResultSetMetaData md = rs.getMetaData();

			int fieldStart = 1;
			int[] fields = null;
			DcObject dco;
			DcModule module = null;
			while (rs.next()) {
				try {
					int moduleIdx = rs.getInt("MODULEIDX");
					module = DcModules.get(moduleIdx);
					fieldStart = 2;
				} catch (Exception e) {
					module = DcModules.get(md.getTableName(1));
				}
				
				if (module == null) {
				    logger.fatal("Could not find module for " + md.getTableName(1));
				    continue;
				}

				if (fields == null) {
					fields = new int[md.getColumnCount() - (fieldStart - 1)];
					int fieldIdx = 0;
					for (int i = fieldStart; i < fields.length + fieldStart; i++) {
						String column = md.getColumnName(i);

						DcField field = module.getField(column);
						
						if (field != null) {
						    fields[fieldIdx++] = field.getIndex();
						} else {
						    logger.error("Could not find field for column " + column + ", the value will be skipped.");
						    fields[fieldIdx++] = -1;
						}
					}
				}

				dco = module.getItem();
				setValues(rs, dco, fields, requestedFields);

				objects.add(dco);
			}
		} catch (Exception e) {
			logger.error("An error occurred while converting result set to items", e);
		}

		try {
			rs.close();
		} catch (Exception e) {
			logger.warn("Failed to close the resultset", e);
		}

		return objects;
	}
	
	public void setValues(ResultSet rs, DcObject item, int[] fields, int[] requestedFields) {
		try {
			Object value = null;
			String column;
			for (int i = 0; i < fields.length; i++) {
			    
			    if (fields[i] == -1)
			        continue;
			    
				DcField field = item.getField(fields[i]);
				column = field.getDatabaseFieldName();

				if (field.isUiOnly())
					continue;

				try {
					value = rs.getObject(column);
					value = CoreUtilities.isEmpty(value) ? null : value;
					item.setValue(fields[i], value);
				} catch (Exception e) {
					logger.error(
							"Could not retrieve and/or set value for field "
									+ field, e);
				}
			}

			item.setValue(Media._SYS_MODULE, item.getModule().getObjectName());

			boolean loan = requestedFields == null;
			boolean images = requestedFields == null;

			if (DatabaseManager.getInstance().isInitialized()) {
			    requestedFields = requestedFields == null ? item.getFieldIndices() : requestedFields;
			    
				for (int field : requestedFields) {
					if (item.getModule().canBeLend() &&
				          (field == DcObject._SYS_AVAILABLE
						|| field == DcObject._SYS_LOANSTATUS
						|| field == DcObject._SYS_LOANSTATUSDAYS
						|| field == DcObject._SYS_LOANDUEDATE 
						|| field == DcObject._SYS_LOANDURATION))
						
						loan |= true;
					else if (	item.getField(field) != null && 
								item.getModule().getIndex() != DcModules._PICTURE &&
							    item.getField(field).getValueType() == DcRepository.ValueTypes._PICTURE)
						images |= true;
					else if (item.getField(field) != null &&
							 item.getField(field).getValueType() == DcRepository.ValueTypes._DCOBJECTCOLLECTION)
						item.initializeReferences(field, false);
				}

				if (loan)
					item.setLoanInformation();

				if (images && item.getModule().isHasImages())
					item.initializeImages();
			}

			item.setNew(false);
			item.markAsUnchanged();

		} catch (Exception e) {
			logger.error(
					"An error occurred while converting result set to items", e);
		}
	}
}
