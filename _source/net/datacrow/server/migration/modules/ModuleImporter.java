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

package net.datacrow.server.migration.modules;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import net.datacrow.core.DcConfig;
import net.datacrow.core.clients.IModuleWizardClient;
import net.datacrow.core.migration.itemimport.ItemImporterHelper;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.modules.ModuleJar;
import net.datacrow.core.modules.ModuleJarException;
import net.datacrow.core.modules.xml.XmlField;
import net.datacrow.core.modules.xml.XmlModule;
import net.datacrow.core.objects.DcObject;
import net.datacrow.core.objects.ValidationException;
import net.datacrow.core.resources.DcResources;
import net.datacrow.core.server.Connector;
import net.datacrow.core.utilities.CoreUtilities;

import org.apache.log4j.Logger;

/**
 * This importer is capable of importing a module and its related information.
 * The importer takes care of clashes with the module indices. The actual data import
 * is managed by the startup process ({@link DcModule#getDefaultData()}).
 * 
 * @author Robert Jan van der Waals
 */
public class ModuleImporter {
    
    private static Logger logger = Logger.getLogger(ModuleImporter.class.getName());

	private File file;
	private Importer importer;
	
	public ModuleImporter(File file) {
		this.file = file;
	}
	
	public void start(IModuleWizardClient client) {
	    Importer importer = new Importer(client, file);
		importer.start();
	}
	
	public void cancel() {
		if (importer != null)
		    importer.cancel();
	}
	
	private class Importer extends Thread {
		
		private boolean canceled = false;
		
		private File file;
		private IModuleWizardClient client;
		
		protected Importer(IModuleWizardClient client, File file) {
			this.client = client;
			this.file = file;
		}
		
		public void cancel() {
			canceled = true;
		}
		
		@Override
		public void run() {
            ZipFile zf = null;
		    
		    try {
		        
		        client.notifyNewTask();
		        
    		    zf = new ZipFile(file);
    
                Collection<ModuleJar> modules = new ArrayList<ModuleJar>();
                Map<String, Collection<File>> icons = new HashMap<String, Collection<File>>();
                Map<String, File> data = new HashMap<String, File>();
                
                Enumeration<? extends ZipEntry> list = zf.entries();
                
                client.notifyTaskStarted(zf.size());
                
                ZipEntry ze;
                BufferedInputStream bis;
                String name;
                String moduleName;
                String imageFileName;
                Collection<File> c;
                int size;
                byte[] bytes;
                File file;
                ModuleJar moduleJar;
                while (list.hasMoreElements() && !canceled) {
                    ze = list.nextElement();
                    bis = new BufferedInputStream(zf.getInputStream(ze));
                    name = ze.getName();
                    
                    client.notify(DcResources.getText("msgProcessingFileX", name));
                    
                    name = name.indexOf("/") > 0 ? name.substring(name.indexOf("/") + 1) : 
                           name.indexOf("\\") > 0 ? name.substring(name.indexOf("\\") + 1) : name;
                    
                    if (name.toLowerCase().endsWith(".xsl")) {
                        // directory name is contained in the name
                        writeToFile(bis, new File(DcConfig.getInstance().getReportDir(), name));
                    } else if (name.toLowerCase().endsWith(".jpg")) {
                        if (!name.startsWith("tbl_"))
                            moduleName = name.substring(0, name.indexOf("_"));
                        else 
                            moduleName = name.substring(0, name.indexOf("_", 4));
                        
                        c = icons.get(moduleName);
                        c = c == null ? new ArrayList<File>() : c;
                        
                        size = (int) ze.getSize();
                        bytes = new byte[size];
                        bis.read(bytes);
                        
                        if (!name.startsWith("tbl_"))
                            imageFileName = name.substring(name.indexOf("_") + 1);
                        else 
                            imageFileName = name.substring(name.indexOf("_", 4) + 1);
                        
                        // store the image to the temp folder
                        c.add(storeImage(moduleName, bytes, imageFileName));
                        icons.put(moduleName, c);
                        
                    } else if (name.toLowerCase().endsWith(".properties")) {
                        writeToFile(bis, new File(DcConfig.getInstance().getModuleDir(), name));
                    } else if (name.toLowerCase().endsWith(".xml")) {
                        file = new File(System.getProperty("java.io.tmpdir"), name);
                        writeToFile(bis, file);
                        file.deleteOnExit();
                        data.put(name.substring(0, name.toLowerCase().indexOf(".xml")), file);
                    } else if (name.toLowerCase().endsWith(".jar")) {
                        // check if the custom module does not already exist
                        if (DcModules.get(name.substring(0, name.indexOf(".jar"))) == null) {
                            writeToFile(bis, new File(DcConfig.getInstance().getModuleDir(), name));
                            
                            moduleJar = new ModuleJar(name);
                            moduleJar.load();
                            modules.add(moduleJar);
                        }
                    }
                    client.notifyProcessed();
                    bis.close();
                }
                
                processData(icons, data);
                reindexModules(modules);

		    } catch (Exception e) {
		        client.notifyError(e);
		    } finally {
		        try {
		            if (zf != null) zf.close();
		        } catch (Exception e) {
		            logger.debug("Could not close zip file", e);
		        }
		    }
		    
		    client.notifyTaskCompleted(true, null);
		}
		
		private void reindexModules(Collection<ModuleJar> modules) {
		    for (ModuleJar mj : modules) {
		        XmlModule xm = mj.getModule();
		        
		        // module does not exist
		        if (DcModules.get(xm.getName()) == null) continue;

	            int oldIdx = xm.getIndex();
	            int newIdx = DcModules.getAvailableIdx(xm);
	            
	            xm.setIndex(newIdx);

	            // module does already exist and needs to be renumbered
                // all references must also be updated..
	            Collection<ModuleJar> others = new ArrayList<ModuleJar>();
	            others.addAll(modules);
	            for (ModuleJar other : others) {
	                if (other != mj) {
    	                XmlModule xmOther = other.getModule();
    	                
    	                if (xmOther.getParentIndex() == oldIdx)
    	                    xmOther.setParentIndex(newIdx);
    
                        if (xmOther.getChildIndex() == oldIdx)
                            xmOther.setChildIndex(newIdx);
    	                
    	                for (XmlField field : xmOther.getFields()) {
    	                    if (field.getModuleReference() == oldIdx)
    	                        field.setModuleReference(newIdx);
    	                }
    	                
    	                try {
    	                    other.save();
    	                } catch (ModuleJarException e) {
    	                    logger.error(e, e);
    	                }
	                }
	            }
	            
	            try {
	                // fake registration to make sure the index is known
	                DcModule module = new DcModule(xm);
	                module.setValid(false);
	                DcModules.register(module);
                    mj.save();
                } catch (ModuleJarException e) {
                    logger.error(e, e);
                }
		    }
		}
		
		private void processData(Map<String, Collection<File>> images, Map<String, File> data) {
		    
		    client.notifyTaskStarted(data.size());
		    
		    client.notify(DcResources.getText("msgProcessingModuleItems"));
		    
		    for (String key : data.keySet()) {
		        DcModule module = DcModules.get(key);
		        
		        client.notify(DcResources.getText("msgProcessingItemsForX", key));
		        
                // for existing module data has to be loaded manually.
                // new modules can use the demo data / default data functionality.
		        if (module != null) {
		            client.notify(DcResources.getText("msgModuleExistsMergingItems"));
		            loadItems(data.get(key), module.getIndex());
		        } else { 
		            try {
		                client.notify(DcResources.getText("msgModuleIsNewCreatingItems"));
		                moveImages(key, images.get(key));
		                CoreUtilities.rename(data.get(key), new File(DcConfig.getInstance().getModuleDir() + "data", key + ".xml"), true);
                    } catch (IOException e) {
                        client.notifyError(e);
                    }
		        }
		        client.notifyProcessed();
		    }
		}
		
		/** 
		 * Moves the images from the temp location to the module data folder.
		 * @param key
		 * @param images
		 */
		private void moveImages(String key, Collection<File> images) {
		    if (images == null) 
		        return;
		    
		    File targetDir = new File(DcConfig.getInstance().getModuleDir() + "data", key.toLowerCase() + "_images");
		    File target;
		    for (File source : images) {
		        target = new File(targetDir, source.getName());
		        try {
		            CoreUtilities.rename(source, target, true);
		        } catch (Exception e) {
		            client.notifyError(e);
		        }
		    }
		}
		
		/**
		 * Stores the image to the temp directory. All files, including the module XML and item exports
		 * are stored here. On the item import the images will be retrieved relatively from the stored location.
		 * For the default module data load (for new modules) the files will be moved from this location.
		 * @param moduleName  the module name
		 * @param imageBytes  the image bytes
		 * @param filename    the image filename
		 * @return
		 */
		private File storeImage(String moduleName, byte[] imageBytes, String filename) {

		    String tempDir = System.getProperty("java.io.tmpdir");
		    File dir = new File(tempDir, moduleName.toLowerCase() + "_images");
		    dir.mkdirs();
		    
		    client.notify(DcResources.getText("msgSavingImagesTo", dir.toString()));
		    File target =  new File(dir, filename);
	        try {
	            CoreUtilities.writeToFile(imageBytes, target);
	            client.notify(DcResources.getText("msgSavingImage", filename));
	        } catch (Exception e) {
	            client.notifyError(e);
	        }
	        
	        return target;
		}
		
		private void loadItems(File file, int moduleIdx) {
		    try {
    	        
		        client.notify("Loading items");
		        
		        ItemImporterHelper reader = new ItemImporterHelper("XML", moduleIdx, file);
                reader.start();
                Collection<DcObject> items = reader.getItems();
                reader.clear();
                
                client.notifyStartedSubProcess(items.size());
                
                int counter = 1;
                Connector connector = DcConfig.getInstance().getConnector();
                for (DcObject item : items) {
                    DcObject other = connector.getItemByKeyword(item.getModule().getIndex(), item.toString());

                    // Check if the item exists and if so, update the item with the found values. Else just create a new item.
                    // This is to make sure the order in which XML files are processed (first software, then categories)
                    // is of no importance (!).
                    try {
                        if (other != null) {
                            client.notify(DcResources.getText("msgItemExistsMerged", other.toString()));
                            other.setLastInLine(counter == items.size());
                            other.merge(item);
                            other.setChanged(DcObject._SYS_CREATED, false);
                            other.setChanged(DcObject._SYS_MODIFIED, false);
                            other.setValidate(false);
                            
                            connector.saveItem(other);
                        } else {
                            client.notify(DcResources.getText("msgItemNoExistsCreated", item.toString()));
                            item.setLastInLine(counter == items.size());
                            item.setValidate(false);
                            
                            connector.saveItem(item);
                        }
                        
                    } catch (ValidationException ignore) {} // cannot occur (for real!)

                    counter++;
                    client.notifySubProcessed();
                }
		    } catch (Exception e) {
		        client.notifyError(e);
		    }
		}
		
		private void writeToFile(InputStream is, File file) throws IOException{
		    
		    if (file.getParentFile() != null) 
		        file.getParentFile().mkdirs();
		    
            FileOutputStream fos = new FileOutputStream(file);
            
            int b;
            while ((b = is.read())!= -1)
                fos.write(b);
            
            fos.flush();
            fos.close();
		}
	}
}
