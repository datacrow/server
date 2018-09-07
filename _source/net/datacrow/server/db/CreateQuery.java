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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcField;
import net.datacrow.core.objects.DcMapping;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.Picture;
import net.datacrow.core.objects.helpers.ExternalReference;

import org.apache.log4j.Logger;

public class CreateQuery extends Query {

    private final static Logger logger = Logger.getLogger(CreateQuery.class.getName());
    
    public CreateQuery(int module) {
        super(null, module);
    }

    @Override
    public List<DcObject> run() {
        Connection conn = null;
        Statement stmt = null;

        DcModule module = getModule();
        String columns = "";
        for (DcField field : module.getFields()) {
            if (!field.isUiOnly()) {
                if (columns.length() > 0)
                    columns += ", ";

                columns += field.getDatabaseFieldName() + " " + field.getDataBaseFieldType();
                if (field.getIndex() == DcObject._ID) {
                    columns += " PRIMARY KEY";
                }
            }
        }
        
        String sql = "CREATE MEMORY TABLE " + module.getTableName() + "\r\n(" + columns + ");";
        
        try { 
            conn = DatabaseManager.getInstance().getAdminConnection();
            stmt = conn.createStatement();
            stmt.execute(sql);
        } catch (SQLException se) {
            if (isLog())
                logger.error(se, se);
        }
        
        if (module.getIndex() == DcModules._PICTURE) {
            try { 
                stmt.execute("CREATE UNIQUE INDEX " + module.getTableName() + "_IDX ON " + module.getTableName() + " (" +
                        module.getField(Picture._A_OBJECTID).getDatabaseFieldName() + ", " +
                        module.getField(Picture._B_FIELD).getDatabaseFieldName() + ")");
            } catch (SQLException se) {
                if (isLog())
                    logger.error(se, se);
            }
        } else if (module.getType() == DcModule._TYPE_MAPPING_MODULE) {
            try { 
                stmt.execute("CREATE UNIQUE INDEX " + module.getTableName() + "_IDX ON " + module.getTableName() + " (" +
                        module.getField(DcMapping._A_PARENT_ID).getDatabaseFieldName() + ", " +
                        module.getField(DcMapping._B_REFERENCED_ID).getDatabaseFieldName() + ")");
            } catch (SQLException se) {
                if (isLog())
                    logger.error(se, se);
            }
        } else if (module.getType() == DcModule._TYPE_EXTERNALREFERENCE_MODULE) {
            try { 
                stmt.execute("CREATE UNIQUE INDEX " + module.getTableName() + "_IDX ON " + module.getTableName() + " (" +
                        module.getField(ExternalReference._EXTERNAL_ID).getDatabaseFieldName() + ", " +
                        module.getField(ExternalReference._EXTERNAL_ID_TYPE).getDatabaseFieldName() + ")");
            } catch (SQLException se) {
                if (isLog())
                    logger.error(se, se);
            }
        }
        
        try {
            if (stmt != null) stmt.close();
        } catch (SQLException e) {
            if (isLog())
                logger.error("Error while closing connection", e);
        }

        clear();
        return null;
    }
}
