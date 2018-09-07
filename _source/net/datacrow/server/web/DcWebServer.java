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
import java.io.IOException;

import net.datacrow.core.DcConfig;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcImageIcon;
import net.datacrow.core.utilities.CoreUtilities;
import net.datacrow.core.utilities.Directory;

import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.apache.log4j.Logger;


/**
 * The web server. This is the wrapper around the Jetty server.  
 * 
 * @author Robert Jan van der Waals
 */
public class DcWebServer {
    
    private static Logger logger = Logger.getLogger(DcWebServer.class.getName());
    
    private static final String context = "/datacrow";
    
	private boolean isRunning;
	private Tomcat server;
	private int port;
	
	/**
	 * Creates a new instance.
	 */
	public DcWebServer(int port) {
	    this.port = port;
	}
	
	/**
	 * Indicates if the server is currently up and running.
	 */
	public boolean isRunning() {
        return isRunning;
    }
	
	public void setup() {
        logger.info("Starting to set up the web root");
        
        File webDir = new File(DcConfig.getInstance().getWebDir(), "datacrow/");
        webDir.mkdirs();
        
        File file;
        File targetDir;
        int idx;
        Directory dir = new Directory(new File(DcConfig.getInstance().getInstallationDir(), "webapp/datacrow").toString(), true, null);
        for (String s : dir.read()) {
            try {
                file = new File(s);
                idx = s.indexOf("webapp/datacrow/") > -1 ? s.indexOf("webapp/datacrow/") : s.indexOf("webapp\\datacrow\\");
                
                if (idx == -1) continue;
                
                targetDir = (new File(webDir, s.substring(idx + "webapp/datacrow/".length())).getParentFile());
                targetDir.mkdirs();
                CoreUtilities.copy(file, new File(targetDir, file.getName()), true);
            } catch (Exception e) {
                logger.error("An error occured while copying file " + s, e);
            }
        }
        
        createIcons();
        createStyleSheet();
        
        logger.info("Web root has been set up");
	}
	
	private void createIcons() {
	    File webDir = new File(DcConfig.getInstance().getWebDir(), "datacrow/");
	    File dir = new File(webDir, "/resources/default/images/");
	    dir.mkdirs();
	    
	    int idx;
	    for (DcModule m : DcModules.getModules()) {
	        idx = m.getIndex();
    	    createIcon(new File(dir, idx + "_16.png").toString(), m.getIcon16());
    	    createIcon(new File(dir, idx + "_32.png").toString(), m.getIcon32());
	    }
	}
	
	private void createStyleSheet() {
	    StringBuffer sb = new StringBuffer();
	    
        File webDir = new File(DcConfig.getInstance().getWebDir(), "datacrow/");
        File dir = new File(webDir, "resources/default/stylesheets/");
	    File editableStyleFile = new File(dir, "editable_style.css");

	    if (editableStyleFile.exists()) {
	        try {
    	        sb.append(new String(CoreUtilities.readFile(editableStyleFile)));
    	        sb.append("\n");
	        } catch (IOException ioe) {
	            logger.warn("Could not load the editable CSS file", ioe);
	        }
	    }
        
        int idx;
        for (DcModule m : DcModules.getModules()) {
            
            if (m.getXmlModule() == null || !m.isTopModule()) continue;
            
            idx = m.getIndex();
            sb.append(".moduleicon" + m.getIndex() + "_16 {\n");
            sb.append("\tbackground: url(\"#{resource['default:images/" + idx + "_16.png']}\")  no-repeat !important;\n");
            sb.append("\twidth:16px;\n");
            sb.append("\theight:16px;\n");
            sb.append("}\n");
            sb.append(".moduleicon" + m.getIndex() + "_32 {\n");
            sb.append("\tbackground: url(\"#{resource['default:images/" + idx + "_32.png']}\")  no-repeat !important;\n");
            sb.append("\twidth:32px;\n");
            sb.append("\theight:32px;\n");
            sb.append("}\n");
        }
        
        try {
            dir.mkdirs();
            File css = new File(dir, "style.css");
            css.delete();
            
            CoreUtilities.writeToFile(sb.toString().getBytes(), css);
        } catch (Exception e) {
            logger.error("Could not create Stylesheet. Style is reset to the default.");
        }
        
	}
	
    private void createIcon(String filename, DcImageIcon icon) {
        
        if (icon == null)
            return;
        
        File file = new File(filename);
        
        if (file.exists()) {
            file.delete();
            file = new File(filename);
        }
            
        file.deleteOnExit();
        
        try {
            byte[] b = icon.getBytes();
            if (b != null)
                CoreUtilities.writeToFile(b, filename);
        } catch (Exception e) {
            logger.error("Could not write icon to disk: " + filename, e);
        }
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
        
        String baseDir = DcConfig.getInstance().getWebDir();
        server.setBaseDir(baseDir);

        File contextDir = new File(baseDir, context);
        Context context = server.addWebapp("/datacrow", contextDir.toString());

        File configFile = new File(new File(baseDir, "datacrow"), "WEB-INF/web.xml");
        context.setConfigFile(configFile.toURI().toURL());
        
        server.start();

        isRunning = true;
	}
}
