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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import net.datacrow.core.DcRepository;
import net.datacrow.core.console.UIComponents;
import net.datacrow.core.data.DataFilter;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcMapping;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.DcProperty;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.server.security.SecurityCenter;

import org.apache.log4j.Logger;

/**
 * Manages table conversions based on the module definition.
 * 
 * @author Robert Jan van der Waals
 */
public class Conversion {

    private static Logger logger = Logger.getLogger(Conversion.class.getName());
    
    private int moduleIdx;
    
    private String columnName;
    
    private int oldFieldType;
    private int newFieldType;
    private int referencingModuleIdx = -1;
    
    public Conversion(String s) {
        StringTokenizer st = new StringTokenizer(s, "/&/");
        List<String> c = new ArrayList<String>();
        while (st.hasMoreTokens())
            c.add((String) st.nextElement());
        
        int i = 0;
        setModuleIdx(Integer.parseInt(c.get(i++)));
        setColumnName(c.get(i++));
        setOldFieldType(Integer.parseInt(c.get(i++)));
        setNewFieldType(Integer.parseInt(c.get(i++)));
        setReferencingModuleIdx(Integer.parseInt(c.get(i++)));
    }
    
    public Conversion(int module) {
        this.moduleIdx = module;
    }
    
    @Override
    public String toString() {
        return  getModuleIdx() + "/&/" + getColumnName() + "/&/" + 
                getOldFieldType() + "/&/" + getNewFieldType() +  "/&/" +
                getReferencingModuleIdx();
    }

    /**
     * Checks whether the conversion is actually needed. This check is in place to make
     * sure older backups can still be restored. 
     */
    public boolean isNeeded() {
        boolean needed = false;
        
        DatabaseManager dm = DatabaseManager.getInstance();
        
        if (DcModules.get(moduleIdx) == null)
            return false;
        
        DcModule refMod = DcModules.get(moduleIdx + referencingModuleIdx) != null ? DcModules.get(moduleIdx + referencingModuleIdx) : DcModules.get(referencingModuleIdx);
        
        // check if the column exists (old version with old module will not have the column)
        if (getNewFieldType() != UIComponents._REFERENCESFIELD) {
            String sql = "select top 1 " + columnName + " from " + DcModules.get(moduleIdx).getTableName();
            try {
                ResultSet rs = dm.executeSQL(getUser(), sql);
                rs.close();
            } catch (Exception se) {
                return false;
            }
        }
        
        try {
            String sql = "select top 1 * from " + DcModules.get(moduleIdx).getTableName();
            ResultSet result = dm.executeSQL(getUser(), sql);
            ResultSetMetaData meta = result.getMetaData();
            
            if (getNewFieldType() == UIComponents._REFERENCESFIELD) {
                boolean exists = false;
                for (int i = 1; i < meta.getColumnCount() + 1; i++)
                    exists |= meta.getColumnName(i).equalsIgnoreCase(columnName);
                
                // column should no longer be there after a successful conversion..
                // else the conversion still needs to (re-) occur.
                needed = exists;
            } else if (getNewFieldType() == UIComponents._REFERENCEFIELD) {
                // Check if there are items stored in the targeted module and if it exists.

                sql = "select top 1 * from " + refMod.getTableName();
                
                try {
                    ResultSet rs = dm.executeSQL(getUser(), sql);
                    rs.close();
                    
                    int pos = -1;
                    for (int idx = 1; idx < meta.getColumnCount(); idx ++) {
                        if (meta.getColumnName(idx).equalsIgnoreCase(columnName))
                            pos = idx;
                    }
                    
                    // check the column type.. if not BIGINT a conversion is still needed.
                    needed = pos > -1 && meta.getColumnType(pos) != Types.BIGINT;
                    
                    if (!needed) {
                        // Check if each of the values actually exists in the reference module!!!!!!
                        sql = "select distinct " + columnName + " from " + DcModules.get(getModuleIdx()).getTableName() + " where " + columnName + " is not null " +
                              "and " + columnName + " not in (select " + refMod.getField(DcProperty._A_NAME).getDatabaseFieldName() + " from " + refMod.getTableName() + ") " +
                              "and " + columnName + " not in (select ID from " + refMod.getTableName() + ")";
                        
                        rs = dm.executeSQL(getUser(), sql);
                        
                        while (rs.next()) {
                            needed = true;
                            break;
                        }
                        
                        rs.close();
                    }
                    
                } catch (Exception ignore) {
                    needed = true;
                }
            } else {
                sql = "select top 1 " + columnName + " from " + DcModules.get(moduleIdx);
                
                try {
                    ResultSet rs = dm.executeSQL(getUser(), sql);
                    rs.close();
                    
                    int pos = -1;
                    for (int idx = 1; idx < meta.getColumnCount(); idx ++) {
                        if (meta.getColumnName(idx).equalsIgnoreCase(columnName))
                            pos = idx;
                    }
                    
                    needed = !isCorrectColumnType(DcModules.get(moduleIdx).getField(columnName).getDataBaseFieldType(), meta.getColumnType(pos));
                } catch (Exception ignore) {
                    needed = true;
                }
            }
            
            result.close();
        } catch (Exception e) {
            logger.error(e, e);
        }
        
        return needed;
    }
    
    /**
     * Handles complex conversions. Simple conversions are executed directly on the database.
     * @see DatabaseManager#initialize()
     * @return
     */
    public boolean execute() {
        
        // Converting a reference field to a multi-reference field
        if (getOldFieldType() == UIComponents._REFERENCEFIELD &&
            getNewFieldType() == UIComponents._REFERENCESFIELD) {
            
            return convertFromRefToMulti();
         
        // Converting any kind of field to a reference field
        } else if (getNewFieldType() == UIComponents._REFERENCESFIELD ||
                   getNewFieldType() == UIComponents._REFERENCEFIELD) {
            
            return convertToRef();
            
        } else {
            
            return convertToText();
        }
    }    
    
    private SecuredUser getUser() {
    	return SecurityCenter.getInstance().getAdmin();
    }
    
    private boolean convertFromRefToMulti() {
        
        DcModule refMod = DcModules.get(moduleIdx + referencingModuleIdx) != null ? DcModules.get(moduleIdx + referencingModuleIdx) : DcModules.get(referencingModuleIdx);
        
        logger.info("Starting to convert reference field [" + columnName + "] to a multi references field");

        String sql = "SELECT ID, " + getColumnName() + " FROM " + DcModules.get(getModuleIdx()).getTableName() + " " +
                     "WHERE " + getColumnName() + " IS NOT NULL AND " + getColumnName() + 
                     " IN (SELECT " + refMod.getField(DcObject._ID) + " FROM " + refMod.getTableName() + ")";
        try {
            ResultSet rs = DatabaseManager.getInstance().executeSQL(getUser(), sql);
            logger.info(sql);
            
            DcModule mappingMod = DcModules.get(DcModules.getMappingModIdx(
                    moduleIdx, refMod.getIndex(), DcModules.get(moduleIdx).getField(columnName).getIndex()));
        
            DcObject mapping = mappingMod.getItem();
            while (rs.next()) {
                String ID = rs.getString(1);
                String referenceID = rs.getString(2);
                mapping.setValue(DcMapping._A_PARENT_ID, ID);
                mapping.setValue(DcMapping._B_REFERENCED_ID, referenceID);
                new InsertQuery(getUser(), mapping).run();
            }
            
            rs.close();
            
            DatabaseManager.getInstance().executeAsAdmin("ALTER TABLE " + DcModules.get(getModuleIdx()).getTableName() + " DROP COLUMN " + getColumnName());
            
        } catch (Exception e) {
            logger.error("Failed to create reference. Conversion has failed. Restart Data Crow to try again.", e);
            return false;
        }
        
        return true;
    }
    
    private boolean convertToRef() {
        
        DcModule refMod = DcModules.get(moduleIdx + referencingModuleIdx) != null ? DcModules.get(moduleIdx + referencingModuleIdx) : DcModules.get(referencingModuleIdx);
        
        logger.info("Starting to convert field [" + columnName + "] to a reference field");
        
        String sql = "select distinct " + columnName + " from " + DcModules.get(getModuleIdx()).getTableName() + " where " + columnName + " is not null";
        
        try {
            ResultSet rs = DatabaseManager.getInstance().executeSQL(getUser(), sql);
            
            while (rs.next()) {
                String name = rs.getString(1);
                
                // check if the referenced item exists
                DcObject reference = refMod.getItem();
                reference.setValue(DcProperty._A_NAME, name);
                
                List<DcObject> items = new SelectQuery(
                		getUser(), 
                		new DataFilter(reference), 
                		new int[] {DcObject._ID}).run();
                
                if (items.size() == 0) {
                    reference.setIDs();
                    new InsertQuery(getUser(), reference).run();
                }
                
                String sql2 = "select item.ID, property.ID from " + refMod.getTableName() + " property " +
                               "inner join " + DcModules.get(getModuleIdx()).getTableName() + " item " +
                               "on CONVERT(property." + refMod.getField(DcProperty._A_NAME).getDatabaseFieldName() + ",LONGVARCHAR) =" +
                               "CONVERT(item." + columnName + ",LONGVARCHAR) and item." + columnName + " = '" + name.replaceAll("'", "''") + "'";
                ResultSet rs2 = DatabaseManager.getInstance().executeSQL(getUser(), sql2);
                
                while (rs2.next()) {
                    String itemID = rs2.getString(1);
                    String propertyID = rs2.getString(2);
                    
                    if (getNewFieldType() == UIComponents._REFERENCESFIELD) {
                        DcModule mappingMod = DcModules.get(DcModules.getMappingModIdx(
                                moduleIdx, refMod.getIndex(), DcModules.get(moduleIdx).getField(columnName).getIndex()));
                        
                        DcObject mapping = mappingMod.getItem();
                        mapping.setValue(DcMapping._A_PARENT_ID, itemID);
                        mapping.setValue(DcMapping._B_REFERENCED_ID, propertyID);
                        
                        items = new SelectQuery(getUser(), mapping, null).run();
                        if (items.size() == 0)
                            new InsertQuery(getUser(), mapping).run();
                        
                    } else {
                        String sql3 = "update " + DcModules.get(getModuleIdx()).getTableName() +
                                      " set " + columnName + "=" + propertyID;
                        DatabaseManager.getInstance().execute(getUser(), sql3);
                    }
                }
                
                rs2.close();
            }

            rs.close();
            
        } catch (Exception e) {
            logger.error("Failed to create reference. Conversion has failed. Restart Data Crow to try again.", e);
            return false;
        }  
        
        try {
            if (getNewFieldType() == UIComponents._REFERENCEFIELD) {
            	DatabaseManager.getInstance().execute(
                		getUser(),
                        "alter table " + DcModules.get(getModuleIdx()).getTableName() + 
                       " alter column " + columnName + " " + DcModules.get(getModuleIdx()).getField(columnName).getDataBaseFieldType());
            } 
            
            // note that column removal is performed by the cleanup method of the database
            
        } catch (Exception e) {
            logger.error("Failed to clean up after doing the field type conversion.", e);
        }            
        
        return true;
    }
    
    private boolean convertToText() {
        try {
            logger.info("Converting " + columnName + " for module " + DcModules.get(moduleIdx).getName() + " to a text column.");
          
            if (DcModules.get(moduleIdx).getField(columnName) != null) {
                String sql = "alter table " + DcModules.get(moduleIdx).getTableName() + " alter column " + columnName + " " +
                             DcModules.get(moduleIdx).getField(columnName).getDataBaseFieldType();
                
                DatabaseManager.getInstance().execute(getUser(), sql);
            }

        } catch (Exception se) {
            logger.error("Could not convert to text!", se);
        }
        
        return true;
    }
    
    private boolean isCorrectColumnType(String dcType, int dbType) {
        if (dbType == Types.BIGINT && 
           (!dcType.startsWith(DcRepository.Database._FIELDBIGINT) &&
            !dcType.startsWith(DcRepository.Database._FIELDNUMERIC))) {
            return false;
        } else if (dbType == Types.VARCHAR && !dcType.startsWith(DcRepository.Database._FIELDSTRING)) {
            return false;
        } else if (dbType == Types.LONGVARCHAR && 
                (!dcType.equals(DcRepository.Database._FIELDOBJECT) && 
                 !dcType.equals(DcRepository.Database._FIELDLONGSTRING))) {
            return false;
        } else if (dbType == Types.DATE && !dcType.equals(DcRepository.Database._FIELDDATE)) {
            return false;
        } else if (dbType == Types.BOOLEAN && !dcType.equals(DcRepository.Database._FIELDBOOLEAN)) {
            return false;
        } else if (dbType == Types.NUMERIC && !dcType.startsWith(DcRepository.Database._FIELDNUMERIC)) {
            return false;
        }
        
        return true;
    }      
    
    public int getReferencingModuleIdx() {
        return referencingModuleIdx;
    }

    public void setReferencingModuleIdx(int referencingModuleIdx) {
        this.referencingModuleIdx = referencingModuleIdx;
    }

    public int getModuleIdx() {
        return moduleIdx;
    }

    public void setModuleIdx(int moduleIdx) {
        this.moduleIdx = moduleIdx;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getOldFieldType() {
        return oldFieldType;
    }

    public void setOldFieldType(int oldFieldType) {
        this.oldFieldType = oldFieldType;
    }

    public int getNewFieldType() {
        return newFieldType;
    }

    public void setNewFieldType(int newFieldType) {
        this.newFieldType = newFieldType;
    }
}
