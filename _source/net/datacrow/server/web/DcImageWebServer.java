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

package net.datacrow.server.web;

import java.io.File;

import net.datacrow.core.DcConfig;

import org.apache.catalina.startup.Tomcat;


/**
 * The web server. This is the wrapper around the Jetty server.  
 * 
 * @author Robert Jan van der Waals
 */
public class DcImageWebServer {
    
	private boolean isRunning;
	private Tomcat server;
	private int port;
	
	/**
	 * Creates a new instance.
	 */
	public DcImageWebServer(int port) {
	    this.port = port;
	}
	
	/**
	 * Indicates if the server is currently up and running.
	 */
	public boolean isRunning() {
        return isRunning;
    }
	
    /**
     * Stops the server.
     * @throws Exception
     */
	public void stop() throws Exception {
	    server.stop();
        isRunning = false;
	}
	
	/**
	 * Starts the Web Server. The port is configurable.
	 */
	public void start() throws Exception {
        server = new Tomcat();
        server.setPort(port);
         
        String baseDir = DcConfig.getInstance().getImageDir();
        File contextDir = new File(baseDir);
        
        server.addWebapp("/", contextDir.toString());
        
        server.start();

        isRunning = true;
	}
}
