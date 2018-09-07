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

import java.util.LinkedList;

import org.apache.log4j.Logger;

/**
 * Queries are queued here awaiting to be executed. The query queue manages the 
 * back log of queries and executes the requests tied to the queries. The queue
 * is based on the FIFO principle.
 * 
 * @author Robert Jan van der Waals
 */
public class QueryQueue extends Thread {

    private static Logger logger = Logger.getLogger(QueryQueue.class.getName());

    private final LinkedList<Query> lQueryQueue = new LinkedList<Query>();
    private boolean isLazy = true;
    
    public QueryQueue() {}

    /**
     * Indicates the back log.
     */
    public int getQueueSize() {
    	return lQueryQueue.size();
    }
    
    /**
     * Add a query to the end of the queue. 
     * @param query
     */
    public void addQuery(Query query) {
        lQueryQueue.addLast(query);
    }

    @Override
    public void run() {
        synchronized(this) {
            while (true) {
                try {
                    if (lQueryQueue.size() > 0 && isLazy) {
                        isLazy = false;
                        Query query = lQueryQueue.removeFirst();
                        query.run();
                    } else {
                        isLazy = true;
                        sleep(10);
                    }
                } catch (Exception e) {
                    logger.error(e, e);
                }
            }
        }
    }
}
