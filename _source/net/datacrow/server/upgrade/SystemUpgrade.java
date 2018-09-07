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

package net.datacrow.server.upgrade;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.datacrow.core.DcConfig;
import net.datacrow.core.Version;
import net.datacrow.core.console.IPollerTask;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.objects.DcField;
import net.datacrow.core.objects.DcImageIcon;
import net.datacrow.core.objects.Picture;
import net.datacrow.core.objects.helpers.MusicTrack;
import net.datacrow.core.resources.DcResources;
import net.datacrow.core.server.Connector;
import net.datacrow.core.utilities.Base64;
import net.datacrow.core.utilities.CoreUtilities;
import net.datacrow.core.utilities.Directory;
import net.datacrow.server.db.DatabaseManager;

import org.apache.log4j.Logger;

/**
 * Upgrade steps for the various versions.
 * 
 * Converts the current database before the actual module tables are created / updated.
 * This means that the code here defies workflow logic and is strictly to be used for
 * table conversions and migration out of the scope of the normal module upgrade code.
 * 
 * The automatic database correction script runs after this manual upgrade.
 * 
 * @author Robert Jan van der Waals
 */
public class SystemUpgrade {
    
    private static Logger logger = Logger.getLogger(SystemUpgrade.class.getName());
    
    private boolean dbInitialized;
    
    public SystemUpgrade(boolean dbInitialized) {
        this.dbInitialized = dbInitialized;
    }
    
    public void start() throws SystemUpgradeException {
        try {
            
            Version v = DatabaseManager.getInstance().getVersion();
            if (!dbInitialized && v.isOlder(new Version(4, 0, 0, 0))) {
                Connector connector = DcConfig.getInstance().getConnector();
                connector.displayMessage("Make sure you have NOT installed Data Crow on top of an "
                        + "older version (3.x) of Data Crow; this will cause errors. This is just "
                        + "for information.");
                
                checkAudioTables();
            }
            
            if (dbInitialized && v.isOlder(new Version(4, 0, 2, 0))) {
                renumberMusicTracks();
            }
            
            if (dbInitialized && v.isOlder(new Version(4, 1, 1, 0))) {
                saveIcons();
            }
            
            if (dbInitialized && v.isOlder(new Version(4, 0, 5, 0))) {
                new File(DcConfig.getInstance().getModuleSettingsDir(), "record label.properties").delete();
            }
            
            if (dbInitialized && v.isOlder(DcConfig.getInstance().getVersion())) {
                File installDirRes = new File(DcConfig.getInstance().getInstallationDir(), "resources/");
                File userDirRes = new File(DcConfig.getInstance().getResourcesDir());
                Directory dir = new Directory(installDirRes.toString(), false, new String[] {"properties"});
                File sourceFile;
                for (String installDirFile : dir.read()) {
                    sourceFile = new File(installDirFile);
                    CoreUtilities.copy(
                            sourceFile, 
                            new File(userDirRes, sourceFile.getName()), 
                            false);
                }
                
                // When this file is removed, the default from the JAR file will be used.
                new File(userDirRes, "English_resources.properties").delete();
                
                // Reload the resources
                new DcResources().initialize();
            }
            
            if (dbInitialized && v.isOlder(DcConfig.getInstance().getVersion())) {
            	File installDirReport = new File(DcConfig.getInstance().getInstallationDir(), "reports/");
                File userDirReport = new File(DcConfig.getInstance().getReportDir());
                Directory dir = new Directory(installDirReport.toString(), true, new String[] {"jasper"});
                File sourceFile;
                File targetFile;
                for (String installDirFile : dir.read()) {
                    sourceFile = new File(installDirFile);
                    targetFile = new File(
                    		new File(userDirReport, sourceFile.getParentFile().getName()), sourceFile.getName());
                    
                    CoreUtilities.copy(sourceFile, targetFile, false);
                }
            }
            
            if (!dbInitialized)
                moveImages();
            
        } catch (Exception e) {
            String msg = e.toString() + ". Data conversion failed. " +
                "Please restore your latest Backup and retry. Contact the developer " +
                "if the error persists";
            throw new SystemUpgradeException(msg);
        }
    }
    
    private void saveIcons() {
        Connection conn = DatabaseManager.getInstance().getAdminConnection();
        Connector connector = DcConfig.getInstance().getConnector();
        connector.displayMessage("Data Crow will now save all icons to the image folder");
        
        String ID;
        String icon;
        for (DcModule m : DcModules.getAllModules()) {
            DcField fld = m.getIconField();
            
            if (fld != null) {
                try {
                    Statement stmt = conn.createStatement();
                    
                    String sql = "SELECT ID, " + fld.getDatabaseFieldName() + " FROM " + 
                                 m.getTableName() + " WHERE " + fld.getDatabaseFieldName() + " IS NOT NULL AND LENGTH(" + fld.getDatabaseFieldName() + ") > 1"; 
                    ResultSet rs = stmt.executeQuery(sql);
                    
                    while (rs.next()) {
                        ID = rs.getString(1);
                        icon = rs.getString(2);

                        File file = new File(DcConfig.getInstance().getImageDir(), "icon_" + ID + ".jpg");
                        CoreUtilities.writeToFile(Base64.decode(icon.toCharArray()), file);
                    }
                } catch (Exception e) {
                    logger.error("Could not save icons for module " + m, e);
                }
            }
            
        }
    }
    
    private void renumberMusicTracks() {
        Connection conn = DatabaseManager.getInstance().getAdminConnection();
        Connector connector = DcConfig.getInstance().getConnector();
        connector.displayMessage("Data Crow will now convert the Music Track numbers");
        
        try {
            Statement stmt = conn.createStatement();
            
            DcModule module = DcModules.get(DcModules._MUSIC_TRACK);
            String fld = module.getField(MusicTrack._F_TRACKNUMBER).getDatabaseFieldName();
            
            String sql = "SELECT ID, " + fld + " FROM " + module.getTableName() + " WHERE " + fld + " IS NOT NULL AND LENGTH(" + fld + ") = 1"; 
            ResultSet rs = stmt.executeQuery(sql);
            
            String ID; 
            String track;
            while (rs.next()) {
                ID = rs.getString(1);
                track = rs.getString(2);
                
                if (Character.isDigit(track.charAt(0))) {
                    sql = "UPDATE " + module.getTableName() + " SET " + fld + " = '0" + track + "' WHERE ID = '" + ID + "'";
                    stmt.execute(sql);
                }
            }
            
            rs.close();
            stmt.close();
            
            connector.displayMessage("The conversion of the Music Track numbers was successfull");
            
        } catch (Exception e) {
            logger.error("Could not update the track numbers.", e);
            connector.displayError("Could not update the track numbers. Error: " + e);
        }
    }
    
    public void moveImages() {
        ImageMover imgMover = new ImageMover();
        imgMover.start();
        
        try {
            imgMover.join();
        } catch (Exception e) {
            logger.error("Error while joining the image mover process with the main Thread", e);
        }
    }
    
    private class ImageMover extends Thread {
        
        @Override
        public void run() {
            
            File dir = new File(DcConfig.getInstance().getDataDir(), "wwwroot/datacrow/mediaimages");
            
            if (!dir.exists()) return;
            
            File targetDir = new File(DcConfig.getInstance().getImageDir());
            
            Connector conn = DcConfig.getInstance().getConnector();
            
            conn.displayMessage("The images will be moved and new scaled version will be created. "
                    + "This process can take up to 10 minutes.");
            
            IPollerTask poller = conn.getPollerTask(this, "Image Moving Task");
            
            if (poller != null) {
                poller.start();
            }
            
            boolean success = true;
            try {
                File f;
                String[] files = dir.list();
                
                int counter = 1;
                for (String file : files) {
                    f = new File(dir,  file);
                    
                    if (f.isDirectory()) continue;
                    
                    if (file.endsWith("_small.jpg")) {
                        f.delete();
                    } else {
                        CoreUtilities.rename(f, new File(targetDir, file), true);
                    }
                    
                    if (poller != null) poller.setText("Moved image " + (counter++) + "/" + files.length);
                }
                
                Picture p = new Picture();
                DcImageIcon icon;
                files = targetDir.list();
                counter = 1;
                for (String file : files) {
                    try {
                        f = new File(targetDir,  file);
                        
                        if (f.isDirectory()) continue;
                        
                        icon = new DcImageIcon(f.toString());
                        CoreUtilities.writeScaledImageToFile(icon, new File(p.getScaledFilename(f.toString())));
                        icon.flush();
                        
                        if (poller != null) poller.setText("Creating thumbnail " + (counter++) + "/" + files.length);
                    } catch (Exception e) {
                        logger.warn(e, e);
                    }
                }
            } catch (Exception e) {
                success = false;
                String msg = e.toString() + ". Images could not be moved from the old (" + dir + ") to the new location (" +
                        DcConfig.getInstance().getImageDir() + ").";
                logger.error(msg, e);
                Connector connector = DcConfig.getInstance().getConnector();
                connector.displayError(msg);
            }
            
            if (success) {
                File f;
                Directory d = new Directory(new File(DcConfig.getInstance().getDataDir(), "wwwroot").toString(), true, null);
                for (String removal : d.read()) {
                    f = new File(removal);
                    
                    if (f.isDirectory()) continue;
                    
                    f.delete();
                }

                dir.delete();
                new File(DcConfig.getInstance().getDataDir(), "wwwroot/").delete();
                
                if (poller != null) {
                    poller.finished(true);
                }
                
                conn.displayMessage("All done! Images have been moved and new scaled versions have been created. Old directory has been removed");
                
                if (new File(DcConfig.getInstance().getDataDir(), "wwwroot/").exists()) 
                    conn.displayMessage("The old folder could not be removed. Please delete the following folder manually: " + 
                                new File(DcConfig.getInstance().getDataDir(), "wwwroot/"));
            }
            
            if (poller != null) {
                poller.finished(true);
            }
        }
    }
    
    private void checkAudioTables() {
        Map<String, String> rename = new HashMap<String, String>();
        rename.put("x_musicalbum_artists", "x_music_album_artists");
        rename.put("x_musicalbum_container", "x_music_album_container");
        rename.put("x_musicalbum_countries", "x_music_album_countries");
        rename.put("x_musicalbum_externalreferences", "x_music_album_externalreferences");
        rename.put("x_musicalbum_genres", "x_music_album_genres");
        rename.put("x_musicalbum_languages", "x_music_album_languages");
        rename.put("x_musicalbum_tags", "x_music_album_tags");
        rename.put("x_musictrack_artists", "x_music_track_artists");
        rename.put("x_musictrack_countries", "x_music_track_countries");
        rename.put("x_musictrack_genres", "x_music_track_genres");
        rename.put("x_musictrack_languages", "x_music_track_languages");
        rename.put("musicalbum", "music_album");
        rename.put("musicalbum_externalreference", "music_album_externalreference");
        rename.put("musicalbum_state", "music_album_state");
        rename.put("musicalbum_storagemedium", "music_album_storagemedium");
        rename.put("musicalbum_template", "music_album_template");
        rename.put("musictrack", "music_track");
        rename.put("musictrack_state", "music_track_state");
        rename.put("musictrack_template", "music_track_template");

        Connection conn = DatabaseManager.getInstance().getAdminConnection();
        
        Connector connector = DcConfig.getInstance().getConnector();
        
        connector.displayMessage("Data Crow will now upgrade the database. The Music Album module and the Audio CD module will be merged.");
        
        try {
            Statement stmt = conn.createStatement();
            
            String sql;
            String tableTarget;
            for (String tableSrc : rename.keySet()) {
                tableTarget = rename.get(tableSrc);
                try {
                	sql = "ALTER TABLE " + tableSrc + " RENAME TO " + tableTarget;
                    stmt.execute(sql);
                    logger.info("Renamed " + tableSrc + " to " + tableTarget);
                } catch (Exception e1) {
                    
                    ResultSet rs = null;
                    try {
                        sql = "SELECT COUNT(*) FROM " + tableTarget;
                        rs = stmt.executeQuery(sql);
                        rs.next();
                        
                        int count = rs.getInt(1);
                        
                        if (count == 0) {
                            sql = "DROP TABLE " + tableTarget;
                            stmt.execute(sql);
                            sql = "ALTER TABLE " + tableSrc + " RENAME TO " + tableTarget;
                            stmt.execute(sql);
                        } else {
                            logger.error("Could not rename " + tableSrc + " to " + tableTarget + 
                                    ". The target already exists and is holding records.", e1);
                        }
                    } catch (Exception e2) {
                        logger.error("Could not rename " + tableSrc + " to " + tableTarget, e1);
                        logger.error("Could not correct the situation", e2);
                    } finally {
                        if (rs != null)
                            rs.close();
                    }
                }
            }
            
            stmt.close();
        } catch (Exception e) {
            logger.fatal("Upgrade failed; existing tables music album tables could not be converted.", e);
            connector.displayError("Upgrade failed; existing tables music album tables could not be converted.");
            System.exit(0);
        }
        
        try {
            Statement stmt = conn.createStatement();
            
            String sql;
            try {
                 sql = "alter table music_album_template add service varchar(255)";
                 stmt.execute(sql);
                 sql = "alter table music_album_template add serviceurl varchar(255)";
                 stmt.execute(sql);
            } catch (Exception e) {}
            
            try {
                sql = "alter table music_album add service varchar(255)";
                stmt.execute(sql);
                sql = "alter table music_album add serviceurl varchar(255)";
                stmt.execute(sql);
           } catch (Exception e) {}
            
            try {
                sql = "alter table music_album_template add service varchar(255)";
                stmt.execute(sql);
                sql = "alter table music_album_template add serviceurl varchar(255)";
                stmt.execute(sql);
            } catch (Exception e) {}
            
            try {
                sql = "alter table audiocd add service varchar(255)";
                stmt.execute(sql);
                sql = "alter table audiocd add serviceurl varchar(255)";
                stmt.execute(sql);
           } catch (Exception e) {}
            
            try {
                sql = "alter table audiocd_template add service varchar(255)";
                stmt.execute(sql);
                sql = "alter table audiocd_template add serviceurl varchar(255)";
                stmt.execute(sql);
            } catch (Exception e) {}
            
            try {
                sql = "alter table music_album add TAGS_PERSIST varchar(256)";
                stmt.execute(sql);
            } catch (Exception e) {}
            
            try {
                sql = "alter table audiocd add TAGS_PERSIST varchar(256)";
                stmt.execute(sql);
            } catch (Exception e) {}
            
            try {
                sql = "alter table audiocd_template add TAGS_PERSIST varchar(256)";
                stmt.execute(sql);
            } catch (Exception e) {}
            
            try {
                sql = "alter table music_album_template add TAGS_PERSIST varchar(256)";
                stmt.execute(sql);
            } catch (Exception e) {}
            
            sql = "insert into music_album_template (" +
                  "artists_persist, container_persist, countries_persist, " +
                  "created, description, ean, externalreferences_persist, genres_persist, id, " +
                  "languages_persist, modified, rating, service, serviceurl, state, " +
                  "tags_persist, title, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, webpage, year, templatename, defaulttemplate) " +
                  "select " +
                  "artists_persist, container_persist, countries_persist, " +
                  "created, description, ean, externalreferences_persist, genres_persist, id, " +
                  "languages_persist, modified, rating, service, serviceurl, state, " +
                  "tags_persist, title, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, webpage, year, templatename, defaulttemplate " +
                  "from audiocd_template";
            
            try {
            	stmt.execute(sql);
            	logger.info("Migrate the music album templates successfully!");
            } catch (Exception e) {}
            
            sql = "insert into music_track( "
                    + "albumid, artists_persist, countries_persist, created, description, genres_persist, id, languages_persist, lyric, modified, "
                    + "playlength, rating, title, track, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, year) "
                    + "select "
                    + "albumid, artists_persist, countries_persist, created, description, genres_persist, id, languages_persist, lyric, modified, "
                    + "playlength, rating, title, track, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, year "
                    + "from audiotrack";
            
            stmt.execute(sql);
            logger.info("Migrate the music tracks successfully!");
            
            sql = "insert into music_track_template ( "
                    + "albumid, artists_persist, countries_persist, created, description, genres_persist, id, languages_persist, lyric, modified, "
                    + "playlength, rating, title, track, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, year, templatename, defaulttemplate) "
                    + "select "
                    + "albumid, artists_persist, countries_persist, created, description, genres_persist, id, languages_persist, lyric, modified, "
                    + "playlength, rating, title, track, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, year, templatename, defaulttemplate "
                    + "from audiotrack_template";
            
            stmt.execute(sql);
            logger.info("Migrate the music track temmplates successfully!");
            
            sql = "update music_track_template set defaulttemplate = FALSE"; 
            
            stmt.execute(sql);
            
            sql = "update music_album_template set defaulttemplate = FALSE"; 
            
            stmt.execute(sql);
            
            String storageMediumId = CoreUtilities.getUniqueID();
            sql = "insert into music_album_storagemedium (ID, name, icon) "
            		+ "values ('" + storageMediumId + "' , 'Audio CD', 'iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYA"
            				+ "AAAf8/9hAAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAA3XAAAN1wFCKJt4AAAAB3RJTUUH1QQBE"
            				+ "TU1Kn0pSwAAAzBJREFUOMttk8tPXGUYxn/fd875OGfOQOdSGChlLg2LirULTWmiG9rEbTWa1pRSdd"
            				+ "MYhLUr/wCNWyi404jYpLps2KhFodi5QCS2LGAIgQQoMIMXmA5zrm5KYojv8sn7y7N4nkdw4oaGBy8"
            				+ "DHwJ9QO6FvAZMA1+Njozl//svTsBjyUTy9du3P9jv6Oh41bKsZoB6vV7b2tp8MjHxTaS6X50bHRkb5"
            				+ "H+cpx7NzX7neV6wtLQULiwshKVSKVxcXAzL5XJYrVZD3/eD2dmZqaHhwaljTjt27u8fEC+d77mRz+f"
            				+ "lwcEBvu8jhEAIgWEYSClpNBoim811JxKJ/YhtvlYslB5oQ8ODl5OJ5Me3+geuLi8vS9/3qdUOOawdUK"
            				+ "sdUK8/R9M1mpRC03TCMKS7u7szn38ceeXihXmtt/fSp3fufJR2HTdVr9dxXQddN2hubiHV1k5raxuJR"
            				+ "JJKdZcm1YTjuPi+T6ot5S4u/m7pQN/ZzrO5SqWCUgrXc4nazayurpIv/AYQ3rj+nsjlzrFSXiYRT+J5"
            				+ "HmfOdJ4G+iSQM01TKaUwTRMhIB6Pky88DiOWfdH3/dbvf7gfxGIxqtUKnufhOA6GYRhATj+OUylFEAT"
            				+ "ouo5t20gpgj//2t8A0DTNAUzHcfA8DyEEvu8LQEhgrdFoeEoplFKcOhWj0Wjw7jvXpVLqma7rq29de1"
            				+ "utr6+jVBOu6+J5HoeHhz6wpgPT29vbyXQ63QrQnmpnd2+Hnp4e8flnX5iAubGxwc8PfySXPYfrukQiEX"
            				+ "Z2d+rAQ62391KlvLpy7Urf1bimaei6jmVa7FX2mJ8v8uTJH1SqFdpaUwghkVLS1dXFvXuT/9SP6p9oxU"
            				+ "Jp8+ULPedjsVhHuivdIoRASg1dN4hGm4nHE9h2FCklR0dHZLNZisXC30+Xnt6/Ozr+pQZQLJQeWBHzTdu"
            				+ "Ons5kMqaUEsdxCIKAMAwBsCyLTCbD3Nwj55dfp2fujo7fBDTtuNPFQunbaHMkOzs7k0ul2pVpmrKlpQXL"
            				+ "sgDY3Nr0Jycnnq+UV74eHRm7+QILxYll6rcG+t+w7cj7uqZfEVJ0CSFEEASbnuf9ZBjG+Mk5/wu6lVFu"
            				+ "Spi0OgAAAABJRU5ErkJggg==')";
            
            stmt.execute(sql);
            logger.info("Created storage medium Audio CD");
            
            sql = "insert into music_album (" +
                    "storagemedium, artists_persist, container_persist, countries_persist, " +
                    "created, description, ean, externalreferences_persist, genres_persist, id, " +
                    "languages_persist, modified, rating, service, serviceurl, state, " +
                    "tags_persist, title, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, webpage, year) " +
                    "select '" +
                    storageMediumId + "', artists_persist, container_persist, countries_persist, " +
                    "created, description, ean, externalreferences_persist, genres_persist, id, " +
                    "languages_persist, modified, rating, service, serviceurl, state, " +
                    "tags_persist, title, userlongtext1, userinteger1, userinteger2, usershorttext1, usershorttext2, webpage, year " +
                    "from audiocd";
            
            stmt.execute(sql);
            logger.info("Migrated the audio CD's successfully");
            
            sql = "select ID, NAME, ICON from audiocd_state";
            ResultSet rs1 = stmt.executeQuery(sql);
            ResultSet rs2;
            String stateName1;
            String stateID1;
            String stateIcon1;
            
            String stateID2;
            while (rs1.next()) {
                stateName1 = rs1.getString("NAME");
                stateID1 = rs1.getString("ID");
                stateIcon1 = rs1.getString("ICON");
                
                if (stateName1 == null)
                	continue;
                
                rs2 = stmt.executeQuery("select ID from music_album_state where UPPER(NAME) = '" + stateName1.replaceAll("'", "''").toUpperCase() + "'");
                if (rs2.next()) {
                    stateID2 = rs2.getString("ID");
                    sql = "update music_album set state = '" + stateID2 + "' where state = '" + stateID1 + "'";
                    stmt.execute(sql);
                } else {
                    sql = "insert into music_album_state (ID, NAME, ICON) values ('" + stateID1 + "', '" + stateName1.replaceAll("'", "''") + 
                            "','" + stateIcon1 + "')";
                    stmt.execute(sql);
                }
                rs2.close();
            }
            
            rs1.close();
            
            sql = "select distinct externalid, id, name, externalidtype from audiocd_externalreference";
            PreparedStatement ps = conn.prepareStatement("insert into music_album_externalreference (externalid, id, name, externalidtype) values (?, ?, ?, ?)");
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                try {
                    ps.setString(1, rs.getString("externalid"));
                    ps.setString(2, rs.getString("id"));
                    ps.setString(3, rs.getString("name"));
                    ps.setString(4, rs.getString("externalidtype"));
                    ps.execute();
                } catch (Exception e) {
                    logger.error("Error while inserting Audio CD external reference. Skipping.");
                }
            }
            
            logger.info("Migrated the music album external references successfully");

            Map<String, String> migrate = new HashMap<String, String>();
            migrate.put("x_audiocd_artists", "x_music_album_artists");
            migrate.put("x_audiocd_container", "x_music_album_container");
            migrate.put("x_audiocd_countries", "x_music_album_countries");
            migrate.put("x_audiocd_externalreferences", "x_music_album_externalreferences");
            migrate.put("x_audiocd_genres", "x_music_album_genres");
            migrate.put("x_audiocd_languages", "x_music_album_languages");
            migrate.put("x_audiocd_tags", "x_music_album_tags");
            migrate.put("x_audiotrack_artists", "x_music_track_artists");
            migrate.put("x_audiotrack_countries", "x_music_track_countries");
            migrate.put("x_audiotrack_languages", "x_music_track_languages");
            migrate.put("x_audiotrack_genres", "x_music_track_genres");
            
            String targetTable = null;
            for (String srcTable : migrate.keySet()) {
            	try {
	                targetTable = migrate.get(srcTable);
	                
	                sql = "select created, modified, objectid, referencedid from " + srcTable;
	                rs = stmt.executeQuery(sql);
	                ps = conn.prepareStatement("insert into " + targetTable + "(created, modified, objectid, referencedid) values (?, ?, ?, ?)");
	                
	                while (rs.next()) {
	                	try {
		                	ps.setDate(1, rs.getDate("created"));
		                	ps.setDate(2, rs.getDate("modified"));
		                	ps.setString(3, rs.getString("objectid"));
		                	ps.setString(4, rs.getString("referencedid"));
		                	ps.execute();
	                	} catch (Exception e) {
	                		logger.info("Skipping invalid reference for " + targetTable);
	                		logger.debug(e, e);
	                	}
	                }
	                
	                logger.info("Migrated the music album external references successfully");

            	} catch (Exception e) {
            		logger.error("migration of " + targetTable + " has failed", e);
            	} finally {
            		if (rs != null) rs.close();
            		if (ps != null) ps.close();
            	}
            }
            
            logger.info("The various music modules have been merged successfully.");
            logger.info("Starting cleanup of the old tables.");
            
            Collection<String> oldTables = new ArrayList<String>();
            oldTables.addAll(migrate.keySet());
            oldTables.add("audiocd");
            oldTables.add("audiocd_state");
            oldTables.add("audiocd_template");
            oldTables.add("audiotrack");
            oldTables.add("audiotrack_template");
            
            for (String oldTable : oldTables) {
            	try {
	                sql = "DROP TABLE " + oldTable;
	                stmt.execute(sql);
	                logger.info("Removed table " + oldTable);
            	} catch (Exception e) {}
            }
            
            logger.info("Cleanup has been completed successfully.");
            
            stmt.close();
            conn.close();
            
            connector.displayMessage("The process has finished. The database will now be restarted. This might take up to 5 minutes");
            
            logger.info("Restarting the database. This will take up to 5 minutes.");
            // closes the database
            DatabaseManager.getInstance().closeDatabases(true);
            DatabaseManager.getInstance().getAdminConnection().close();
            
            logger.info("Restart was successful.");
            
            connector.displayMessage("The upgrade was successfull.");
            
        } catch (Exception e) {
            logger.fatal("Upgrade failed; existing tables music album tables could not be converted.", e);
            connector.displayError("Upgrade failed; existing tables music album tables could not be converted.");
            System.exit(0);
        } 
    }
}
