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
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import net.datacrow.core.DcConfig;
import net.datacrow.core.DcRepository;
import net.datacrow.core.clients.IBackupRestoreClient;
import net.datacrow.core.resources.DcResources;
import net.datacrow.core.utilities.Directory;
import net.datacrow.server.db.DatabaseManager;
import net.datacrow.settings.DcSettings;

import org.apache.log4j.Logger;

import de.schlichtherle.truezip.file.TArchiveDetector;
import de.schlichtherle.truezip.file.TConfig;
import de.schlichtherle.truezip.file.TFile;
import de.schlichtherle.truezip.file.TFileWriter;
import de.schlichtherle.truezip.file.TVFS;
import de.schlichtherle.truezip.fs.archive.zip.JarDriver;
import de.schlichtherle.truezip.socket.sl.IOPoolLocator;

/**
 * Performs a backup of the Data Crow data, settings, modules and reports.
 * 
 * @author Robert Jan van der Waals
 */
public class Backup extends Thread {
    
    private static Logger logger = Logger.getLogger(Backup.class.getName());

    private File directory;
    private IBackupRestoreClient client;
    private String comment;
    
    private TArchiveDetector ad = new TArchiveDetector("zip", new JarDriver(IOPoolLocator.SINGLETON));
 
    /**
     * Creates a new instance.
     * @param client The listener which will be informed of events and errors.
     * @param directory The directory where the backup will be created.
     */
    public Backup(IBackupRestoreClient client, File directory, String comment) {
        this.directory = directory;
        this.comment = comment;
        this.client = client;
    }
    
    /**
     * Retrieves all the files to be backed up.
     * @return A collection of fully classified filenames.
     */
    private Collection<String> getFiles() {
        
        Collection<String> files = new ArrayList<String>();
        String paths[] = {
                DcConfig.getInstance().getApplicationSettingsDir(),
                DcConfig.getInstance().getModuleSettingsDir(),
                DcConfig.getInstance().getDatabaseDir(),
                DcConfig.getInstance().getModuleDir(),
                DcConfig.getInstance().getReportDir(),
                DcConfig.getInstance().getResourcesDir(),
                DcConfig.getInstance().getUpgradeDir(),
                DcConfig.getInstance().getImageDir()};
        
        Directory dir;
        for (String path : paths) {
            dir = new Directory(path, true, null);
            files.addAll(dir.read());
        }
        return files;
    }

    private String getZipFile(String target) {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd_HHmm");
        String date = format.format(cal.getTime());

        String filename = "datacrow_backup_" + date + ".zip";
        String zipFile = target.endsWith(File.separator) ? target + filename :
                                                           target + File.separator + filename;
        return zipFile;
    }    
    
    private void addEntry(String zipName, String source) {
        try {
            TConfig.get().setArchiveDetector(ad);
            TFile src = new TFile(source, ad);
            TFile dst = new TFile(zipName, ad);
            src.cp_rp(dst);
        } catch (IOException e) {
            client.notifyError(e);
        }
    }
    
    /**
     * Performs the actual back up and informs the clients on the progress.
     */
    @Override
    public void run() {
        
        if (!directory.exists())
            directory.mkdirs();
        
        client.notify(DcResources.getText("msgStartBackup"));
        client.notify(DcResources.getText("msgClosingDb"));
        
        DatabaseManager.getInstance().closeDatabases(true);

        Collection<String> files = getFiles();
        client.notifyTaskStarted(files.size());
        
        try {
            String zipFileName = getZipFile(directory.toString());
            
            File entry = new TFile(zipFileName + File.separator +  "version.txt", ad);
            Writer writer = new TFileWriter(entry);
            try {
                writer.write(DcConfig.getInstance().getVersion().toString());
                if (comment.length() > 0)
                    writer.write("\n" + comment);
            } catch (IOException e) {
                client.notifyError(e);
            } finally {
                writer.close();
            }
            
            String name;
            for (String filename : files) {
                
                name =  filename.substring(DcConfig.getInstance().getDataDir().length() - 
                        (DcConfig.getInstance().getDataDir().startsWith("/") && !filename.startsWith("/") ? 2 : 1));
                
                while (name.startsWith("/") || name.startsWith("\\"))
                    name = name.substring(1);
                
                client.notifyProcessed();

                addEntry(zipFileName + File.separator + name, filename);
                
                client.notify(DcResources.getText("msgCreatingBackupOfFile", filename));
                
                try {
                    sleep(10);
                } catch (Exception e) {
                    logger.warn(e, e);
                }
            }
            
            client.notify(DcResources.getText("msgWritingBackupFile"));
            
            TVFS.umount();
            
            client.notifyWarning(DcResources.getText("msgBackupFinished"));
            
        } catch (Exception e) {
            client.notify(DcResources.getText("msgBackupError", e.getMessage()));
            client.notifyError(e);
            client.notifyWarning(DcResources.getText("msgBackupFinishedUnsuccessful"));
            client.notify(DcResources.getText("msgBackupFinished"));
        }
        
        DcSettings.set(DcRepository.Settings.stBackupLocation, directory.toString());
        
        client.notify(DcResources.getText("msgRestartingDb"));
        
        DatabaseManager.getInstance().initialize();
        client.notifyTaskCompleted(true, null);
        
        client = null;
        directory = null;
    }
}
