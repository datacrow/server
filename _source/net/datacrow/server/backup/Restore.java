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

package net.datacrow.server.backup;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import net.datacrow.core.DcConfig;
import net.datacrow.core.Version;
import net.datacrow.core.clients.IBackupRestoreClient;
import net.datacrow.core.resources.DcResources;
import net.datacrow.server.db.DatabaseManager;

import org.apache.log4j.Logger;

import de.schlichtherle.truezip.file.TArchiveDetector;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileReader;
import de.schlichtherle.truezip.file.TVFS;
import de.schlichtherle.truezip.fs.archive.zip.JarDriver;
import de.schlichtherle.truezip.socket.sl.IOPoolLocator;

/**
 * The restore class is capable of restoring a back up.
 * Based on the settings either the data, the modules, the modules or all
 * information is restored.
 * 
 * @author Robert Jan van der Waals
 */
public class Restore extends Thread {
    
    private static Logger logger = Logger.getLogger(Restore.class.getName());
    
    private Version version;
    private File source;
    private IBackupRestoreClient client;
    
    private TArchiveDetector ad = new TArchiveDetector("zip", new JarDriver(IOPoolLocator.SINGLETON));
    
    /**
     * Creates a new instance.
     * @param listener The listener will be updated on events and errors.
     * @param source The backup file.
     */
    public Restore(IBackupRestoreClient listener, File source) {
        this.source = source;
        this.client = listener;
        
        if (source.getName().endsWith(".bck")) {
            File tmp = new File(source.toString().replace(".bck", ".zip"));
            new File(source.toString()).renameTo(tmp);
            
            this.source = tmp;
        }
    }
    
    private boolean isVersion(String filename) {
        return filename.toLowerCase().endsWith("version.txt");
    }
    
    private void restartApplication() {
        client.notifyWarning(DcResources.getText("msgRestoreFinishedRestarting"));
        System.exit(0);
    }    
    
    private boolean isSupportedVersion() {
        
        boolean supported = false;
        
    	// This should only be executed server side, never remotely.
        File entry = new TFile(source.toString() + File.separator + "version.txt", ad);
        Reader reader = null;
        try {
        	reader = new TFileReader(entry);
        	
            int data = reader.read();
            StringBuffer sb = new StringBuffer();
            while(data != -1){
                sb.append((char) data);
                data = reader.read();
            }
            
            String s = sb.toString();
            s = s.indexOf("\n") > -1 ? s.substring(0, s.indexOf("\n")) : s;
            version = new Version(s);
        } catch (IOException e) {
        	logger.error(e, e);
        	client.notifyError(e);
        } finally {
            if (reader != null) {
                try { 
                    reader.close(); 
                } catch (IOException e) {
                    logger.debug("An error occured while closing resources", e);
                }
            }
        }
        
        if (version == null || version.isUndetermined()) {
        	client.notifyWarning(DcResources.getText("msgCouldNotDetermineVersion"));
        } else if (version != null && version.isOlder(new Version(3, 4, 13, 0))) {
        	client.notifyWarning(DcResources.getText("msgOldVersion3.4.12"));
        } else if (version != null && version.isOlder(new Version(3, 8, 16, 0))) {
        	client.notifyWarning(DcResources.getText("msgOldVersion3.8.16"));
        } else if (version != null && version.isOlder(new Version(3, 12, 5, 0))) {
        	client.notifyWarning(DcResources.getText("msgOldVersion3.12.5"));
        } else {
            supported = true;
        }
        
        return supported;
    }
    
    private void clear() {
        client.notifyTaskCompleted(true, null);
        version = null;
        source = null;
        client = null;
    }
    
    /**
     * Returns the target file for the provided backup file entry.
     */
    private String getTargetFile(String filename) {
        boolean restore = true;
        if (    !client.isRestoreDatabases() && 
               (filename.toLowerCase().startsWith("database/") ||
                filename.toLowerCase().startsWith("database\\")  ||
                filename.toLowerCase().indexOf("/mediaimages/") > -1 ||
                filename.toLowerCase().indexOf("\\mediaimages\\") > -1)) {
            restore = false;
        } else if (
                !client.isRestoreModules() && 
               (filename.toLowerCase().startsWith("modules/") ||
                filename.toLowerCase().startsWith("modules\\"))) {    
            restore = false;
        } else if (
                !client.isRestoreReports() && 
               (filename.toLowerCase().startsWith("reports/") ||
                filename.toLowerCase().startsWith("reports\\") ))   {
            restore = false;
        } else if ( filename.toLowerCase().endsWith(".log") || 
                    filename.toLowerCase().endsWith("version.properties") ||
                    filename.toLowerCase().endsWith("log4j.properties") ||
                    filename.toLowerCase().contains("datacrow.log")) {
            restore = false;
        }
        
        if (filename.toLowerCase().contains("wwwroot") && !filename.toLowerCase().contains("mediaimages")) {
            restore = false;
        }
   
        return restore ? new File(DcConfig.getInstance().getDataDir(), filename).toString() : null;
    }  
    
    private List<TFile> getContent(TFile parent) {
        List<TFile> files = new ArrayList<TFile>();

        String name;
        TFile jar;
        for (TFile child : parent.listFiles()) {
            
            if (child.isArchive()) {
                files.add(child);
            } else if (child.isFile()) {
                name = child.toString();
                if (name.contains(".jar")) {
                    jar = new TFile(name.substring(0, name.indexOf(".jar") + 4), ad);
                    if (!files.contains(jar)) 
                        files.add(jar);
                } else {
                    files.add(child);
                }
            } else if (child.isDirectory()) {
                files.addAll(getContent(child));
            }
        }
        
        return files;
    }
    
    private boolean restore() throws Exception {
        
        boolean success = true;
        
        TFile zipFile = new TFile(source, ad);
        List<TFile> entries = getContent(zipFile);
        
        client.notifyTaskStarted(entries.size());
        client.notify(DcResources.getText("msgStartRestore"));
        client.notify(DcResources.getText("msgClosingDb"));
        
        DatabaseManager.getInstance().closeDatabases(false);

        String filename;
        File destFile;
        for (TFile entry : entries) {
            
            client.notifyProcessed();
            
            // the filename will contain the full zip file name and thus needs to be stripped
            filename = entry.toString();
            
            if (filename.endsWith(".zip")) continue;
            
            filename = filename.substring(filename.indexOf(".zip") + 5);
            
            client.notify(DcResources.getText("msgRestoringFile", entry.getName()));
            
            try {                    
                filename = getTargetFile(filename);
                
                if (filename == null) continue;
                
                if (isVersion(filename)) continue;
                
                destFile = new File(filename);
                
                if (destFile.exists()) destFile.delete();
                if (destFile.exists()) 
                    client.notify(DcResources.getText("msgRestoreFileOverwriteIssue", entry.getName()));
                
                try {
                    destFile.getParentFile().mkdirs();
                } catch (Exception e) {
                    logger.warn("Unable to create directories for " + filename, e);
                }
                
                if (entry.isArchive()) {
                    entry.cp_rp(new TFile(destFile.toString() + "/", ad));
                } else {
                    entry.cp_rp(destFile);
                }
            
                try {
                    sleep(10);
                } catch (Exception e) {
                    logger.warn(e, e);
                }
                
            } catch (Exception exp) {
                success = false;
                logger.error(exp, exp);
                client.notify(DcResources.getText("msgRestoreFileError", new String[] {filename, exp.getMessage()}));
            }
        }
        
        TVFS.umount();
        
        return success;
    }
    
    /**
     * Performs the actual restore. The listener is updated on errors and events.
     */
    @Override
    public void run() {
        boolean success = false;

        try {
            if (!isSupportedVersion()) {
                clear();
                return;
            }
            
            success = restore();
            
        } catch (Exception e) {
            client.notifyError(e);
        }

        if (success) {
            restartApplication();
            client.notify(DcResources.getText("msgRestoreFinished"));
        } else {
            client.notifyError(new Exception(DcResources.getText("msgIncompleteRestore")));
        }
        
        clear();
    }
}
