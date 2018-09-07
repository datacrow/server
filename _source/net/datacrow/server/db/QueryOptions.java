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

import net.datacrow.core.objects.DcField;

/**
 * Additional query options such as the ordering.
 * 
 * @author Robert Jan van der Waals
 */
public class QueryOptions {

    private String[] ordering;
    private boolean bPreciseSelect;
    private boolean bComplyToAllConditions;

    /**
     * Creates a new instance.
     * @param ordering The ordering to applied.
     * @param bComplyToAllConditions All conditions have to be met.
     * @param bPreciseSelect Only Exact matches only.
     */
    public QueryOptions(DcField[] ordering, boolean bComplyToAllConditions, boolean bPreciseSelect) {
        if (ordering != null) {
            this.ordering = new String[ordering.length];
            for (int i = 0; i < ordering.length; i++) {
                this.ordering[i] = ordering[i].getDatabaseFieldName();
            }
        } else {
            this.ordering = new String[0];
        }

        this.bComplyToAllConditions = bComplyToAllConditions;
        this.bPreciseSelect = bPreciseSelect;
    }

    /**
     * The ordering to be applied when querying for the items.
     * @return
     */
    public String[] getOrdering() {
        return ordering;
    }

    /**
     * Indicates if all the conditions should be matched.
     * @return
     */
    public boolean getComplyAllConditions() {
        return bComplyToAllConditions;
    }

    /**
     * Indicates if matches have to be precise (equals or like condition).
     */
    public boolean getPreciseSelect() {
        return bPreciseSelect;
    }
}
