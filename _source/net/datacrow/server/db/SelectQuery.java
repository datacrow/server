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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import net.datacrow.core.data.DataFilter;
import net.datacrow.core.data.DataFilterConverter;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.server.data.DataManager;

import org.apache.log4j.Logger;

public class SelectQuery extends Query {
    
    private final static Logger logger = Logger.getLogger(SelectQuery.class.getName());
    
    private int[] fields;
    private DataFilter df;
    
    /**
     * Constructs a new Query object from a data filter.
     */
    public SelectQuery(SecuredUser su, DcObject dco, int[] fields) {
        super(su, dco.getModule().getIndex());
        this.fields = fields;
        this.df = new DataFilter(dco);
    }
    
    /**
     * Constructs a new Query object from a data filter.
     */
    public SelectQuery(SecuredUser su, DataFilter df, int[] fields) {
        super(su, df.getModule());
        this.fields = fields;
        this.df = df;
    }
    
    @Override
    public List<DcObject> run()  {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        List<DcObject> items = new ArrayList<DcObject>();
        
        DataFilterConverter dfc = new DataFilterConverter(df);
        String sql = dfc.toSQL(fields, true, true);
        
        logger.debug(sql);
        
        try {
            conn = DatabaseManager.getInstance().getAdminConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            items.addAll(DataManager.getInstance().convert(rs, fields));
            setSuccess(true);
            
        } catch (SQLException e) {
            logger.error("Error (" + e +") while executing query: " + sql, e);
            setSuccess(false);
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                logger.error("Error while closing connection", e);
            }
        }
        
        return items;
    }

    @Override
    public void clear() {
        super.clear();
        fields = null;
        df = null;
    }
    
    @Override
    protected void finalize() throws Throwable {
        clear();
        super.finalize();
    }
}
