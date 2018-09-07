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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.ImageIcon;

import net.datacrow.core.DcRepository;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcField;
import net.datacrow.core.objects.DcMapping;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.Picture;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.core.utilities.CoreUtilities;

import org.apache.log4j.Logger;


public class UpdateQuery extends Query {
    
    private final static Logger logger = Logger.getLogger(UpdateQuery.class.getName());
    
    private DcObject dco;

    public UpdateQuery(SecuredUser su, DcObject dco) {
        super(su, dco.getModule().getIndex());
        this.dco = dco;
    }
    
    @Override
    protected void clear() {
        super.clear();
        dco = null;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public List<DcObject> run() {
        
        Collection<Picture> pictures = new ArrayList<Picture>();
        Collection<Collection<DcMapping>> references = new ArrayList<Collection<DcMapping>>();
        Collection<Object> values = new ArrayList<Object>();
        
        // create non existing references
        createReferences(dco);

        PreparedStatement ps = null;
        Connection conn = null;
        Statement stmt = null;
        
        try {
            conn = DatabaseManager.getInstance().getConnection(getUser());
            stmt = conn.createStatement();

            StringBuffer sbValues = new StringBuffer();
            Picture picture;
            ImageIcon icon;
            Collection<DcMapping> c;
            DcModule mappingMod;
            String sql;
            String s;
            for (DcField field : dco.getFields()) {

                // Make sure only changed fields are updated
                if (!dco.isChanged(field.getIndex()))
                    continue;
                
                if (field.getValueType() == DcRepository.ValueTypes._PICTURE) {
                    picture = (Picture) dco.getValue(field.getIndex());
                    if (picture != null && (picture.isNew() || picture.isEdited() || picture.isDeleted())) {
                        picture.setValue(Picture._A_OBJECTID, dco.getID());
                        picture.setValue(Picture._B_FIELD, field.getDatabaseFieldName());
                        picture.setValue(Picture._C_FILENAME, dco.getID() + "_" + field.getDatabaseFieldName() + ".jpg");
                        
                        icon = (ImageIcon) picture.getValue(Picture._D_IMAGE);
                        if (icon != null) {
                            picture.setValue(Picture._E_HEIGHT, Long.valueOf(icon.getIconHeight()));
                            picture.setValue(Picture._F_WIDTH, Long.valueOf(icon.getIconWidth()));
                            pictures.add(picture);
                        }
                    }
                } else if (field.getValueType() == DcRepository.ValueTypes._DCOBJECTCOLLECTION) {
                    c = (Collection<DcMapping>) dco.getValue(field.getIndex());
                    
                    if (c != null) references.add(c);
                    
                    if (dco.isChanged(field.getIndex())) {
                        mappingMod = DcModules.get(DcModules.getMappingModIdx(field.getModule(), field.getReferenceIdx(), field.getIndex()));
                        sql = "DELETE FROM " + mappingMod.getTableName() + " WHERE " +  
                                     mappingMod.getField(DcMapping._A_PARENT_ID).getDatabaseFieldName() + " = '" + dco.getID() + "'";
                        stmt.execute(sql);
                    }
                } else if (dco.isChanged(field.getIndex()) && !field.isUiOnly()) {
                    if (sbValues.length() > 0)
                        sbValues.append(", ");
    
                    sbValues.append(field.getDatabaseFieldName());
                    sbValues.append(" = ?");
                    values.add(getQueryValue(dco, field.getIndex()));
                }
            }
    
            s = sbValues.toString();
            if (dco.getModule().getIndex() != DcModules._PICTURE && dco.getModule().getType() != DcModule._TYPE_MAPPING_MODULE && !CoreUtilities.isEmpty(values)) {
                ps = conn.prepareStatement("UPDATE " + dco.getTableName() + " SET " + s + "\r\n WHERE ID = '" + dco.getID() + "'");
                setValues(ps, values);
                ps.execute();
                ps.close();
            } else if (!CoreUtilities.isEmpty(values)) {
                ps = conn.prepareStatement("UPDATE " + dco.getTableName() + " SET " + s + "\r\n WHERE " +
                        dco.getDatabaseFieldName(Picture._A_OBJECTID) + " = '" + dco.getValue(Picture._A_OBJECTID) + "' AND " +
                        dco.getDatabaseFieldName(Picture._B_FIELD) + " = '" + dco.getValue(Picture._B_FIELD) + "'");
                setValues(ps, values);
                ps.execute();
                ps.close();
            }
    
            for (Collection<DcMapping> mappings : references) {
                saveReferences(mappings, dco.getID());
            }
            
            for (Picture p : pictures) {
                if (p.isNew()) {
                    // prevent primary key violations
                    stmt.execute("DELETE FROM PICTURE WHERE OBJECTID = '" + 
                                  p.getValue(Picture._A_OBJECTID) + "' AND FIELD = '" + 
                                  p.getValue(Picture._B_FIELD) + "'");
                    new InsertQuery(getUser(), p).run();
                    saveImage(p);
                } else if (p.isEdited()) {
                    new UpdateQuery(getUser(), p).run();
                    saveImage(p);
                } else if (p.isDeleted()) {
                    stmt.execute("DELETE FROM " + p.getTableName() + " WHERE " +
                            p.getField(Picture._A_OBJECTID).getDatabaseFieldName() + " = '" + dco.getID() + "' AND " +
                            p.getField(Picture._B_FIELD).getDatabaseFieldName() + " = '" +  p.getValue(Picture._B_FIELD) + "'");
                    deleteImage(p);    
                }
            }
            
            for (DcField f : dco.getFields()) {
                if (dco.isChanged(f.getIndex()) &&
                    f.getValueType() == DcRepository.ValueTypes._ICON) {
                    saveIcon((String) dco.getValue(f.getIndex()), f, dco.getID());
                }
            }

            if (dco.getDeleteExistingChildren()) {
                DatabaseManager.getInstance().executeSQL(getUser(),
                        "DELETE FROM " + dco.getModule().getChild().getTableName() + " WHERE " + 
                        dco.getModule().getChild().getField(dco.getModule().getChild().getParentReferenceFieldIndex()).getDatabaseFieldName() + " = '" + dco.getID() + "'");
            }
            
            boolean exists = false;
            ResultSet rs;
            Query query;
            for (DcObject child : dco.getCurrentChildren()) {
                if (child.isChanged()) {
                    exists = false;
                    if (child.getID() != null) {
                        rs = DatabaseManager.getInstance().executeSQL(getUser(), "select count(*) from " + 
                                child.getModule().getTableName() + " where ID = '" + child.getID() + "'");
                        rs.next();
                        exists = rs.getInt(1) > 0;
                        rs.close();
                    }
                    
                    query = exists ? new UpdateQuery(getUser(), child) : new InsertQuery(getUser(), child);
                    query.run();
                }
            }
            
            setSuccess(true);
            pictures.clear();
        } catch (SQLException e) {
            setSuccess(false);
            logger.error("An error occured while running the query", e);
        }
        
        try {
            if (stmt != null) stmt.close();
        } catch (SQLException e) {
            logger.error("Error while closing connection", e);
        }
        
        return null;
    }
    
        
    @Override
    protected void finalize() throws Throwable {
        clear();
        super.finalize();
    }
}
