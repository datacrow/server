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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.datacrow.core.DcConfig;
import net.datacrow.core.clients.IItemExporterClient;
import net.datacrow.core.clients.IModuleWizardClient;
import net.datacrow.core.data.DataFilter;
import net.datacrow.core.migration.itemexport.ItemExporterSettings;
import net.datacrow.core.migration.itemexport.XmlExporter;
import net.datacrow.core.modules.DcModule;
import net.datacrow.core.modules.DcModules;
import net.datacrow.core.modules.ExternalReferenceModule;
import net.datacrow.core.modules.xml.XmlModule;
import net.datacrow.core.reporting.Reports;
import net.datacrow.core.resources.DcResources;
import net.datacrow.core.security.SecuredUser;
import net.datacrow.core.utilities.Directory;
import net.datacrow.core.utilities.CoreUtilities;
import net.datacrow.core.utilities.zip.ZipFile;
import net.datacrow.server.data.DataManager;

/**
 * This exporter is capable of exporting a module and the related information.
 * The related custom modules and the data can be exported (based on the user settings).
 * The exported module zip file can be imported using the ModuleImporter class.
 * 
 * @author Robert Jan van der Waals
 */
public class ModuleExporter {

	private int module;
	
	private boolean exportData = false;
	private boolean exportDataRelatedMods = false;
	
	private File path;
	
	private Exporter exporter;
	
	/**
	 * Creates a new instance for the specific module. 
	 * The module must be a main module ({@link DcModule#isTopModule()}).
	 * @param module Main module
	 * @param path The export path
	 */
	public ModuleExporter(int module, File path) {
		this.module = module;
		this.path = path;
	}

    /**
     * Indicates whether the data (items) of the main module should be exported.
     * @param exportRelatedMods
     */	
	public void setExportData(boolean exportData) {
		this.exportData = exportData;
	}

    /**
     * Indicates whether the data (items) of the related modules should be exported.
     * @param exportRelatedMods
     */ 	
	public void setExportDataRelatedMods(boolean exportDataRelatedMods) {
		this.exportDataRelatedMods = exportDataRelatedMods;
	}
	
	/**
	 * The main module index.
	 */
	public int getModule() {
		return module;
	}

	/**
	 * The export path.
	 */
	public File getPath() {
		return path;
	}

	public boolean isExportData() {
		return exportData;
	}

	public boolean isExportDataRelatedMods() {
		return exportDataRelatedMods;
	}

	public void start(IModuleWizardClient client) {
		Exporter exporter = new Exporter(client, this);
		exporter.start();
	}
	
	public void cancel() {
		if (exporter != null)
			exporter.cancel();
	}
	
	private class Exporter extends Thread implements IItemExporterClient {
		
		private IModuleWizardClient client;
		private ModuleExporter parent;
		
		private boolean canceled = false;
		
		protected Exporter(IModuleWizardClient client, ModuleExporter parent) {
			this.client = client;
			this.parent = parent;
		}
		
		protected void cancel() {
			canceled = true;
		}

		@Override
		public void notifyTaskCompleted(boolean success, String taskID) {
			client.notifyTaskCompleted(success, taskID);
		}
		
		@Override
		public void run() {
		    
		    client.notifyNewTask();
		    
			Collection<DcModule> modules = new ArrayList<DcModule>();
			DcModule main = DcModules.get(module); 
			main = main == null ? DcModules.getPropertyBaseModule(module) : main;
			modules.add(main);

			for (DcModule reference : DcModules.getReferencedModules(module))
			    if (!reference.isAbstract() && reference.getIndex() != DcModules._CONTACTPERSON)
			        modules.add(reference);

	        if (main.isParentModule()) {
                modules.add(main.getChild());
                for (DcModule reference : DcModules.getReferencedModules(main.getChild().getIndex())) {
                    if (!modules.contains(reference) && !reference.isAbstract() && 
                         reference.getIndex() != DcModules._CONTACTPERSON) {
                        modules.add(reference);
                    }
                }
	        }
			
			client.notifyTaskStarted(modules.size());
			
			try {
			    
				ZipFile zf = new ZipFile(new File(parent.getPath(), main.getName().toLowerCase() + "_export.zip"));
			
				File file;
				byte[] content;
				for (DcModule module : modules) {
					
					if (canceled) break;
					
					// only export custom modules
					// adds the module jar file to the distribution
					if (module.isCustomModule() && module.getXmlModule() != null) {
    					XmlModule xmlModule = module.getXmlModule();
						String jarName = xmlModule.getJarFilename();
						content = CoreUtilities.readFile(new File(DcConfig.getInstance().getModuleDir(), jarName));
						zf.addEntry(jarName, content);
					}

					// settings export
					file = module.getSettings().getSettings().getSettingsFile();
					module.getSettings().save();
					if (file.exists()) {
					    content = CoreUtilities.readFile(file);
					    zf.addEntry(file.getName(), content);
					}
					
					// reports
					Reports rt = new Reports();
					if (rt.hasReports(module.getIndex())) {
					    String reportDir = DcConfig.getInstance().getReportDir() + module.getName().toLowerCase().replaceAll("[/\\*%., ]", "");
				        Directory dir = new Directory(reportDir, true, new String[] {"jasper"});
				        String name;
					    for (String filename : dir.read()) {
					        content = CoreUtilities.readFile(new File(filename));
					        name = filename.substring(filename.indexOf(File.separator + "reports" + File.separator) + 9);
		                    zf.addEntry(name, content);
					    }
					}
					
					// item export
					if (!module.isChildModule() &&
                        (((module.getIndex() == parent.getModule() || module instanceof ExternalReferenceModule) && parent.isExportData()) ||
 					    (!(module instanceof ExternalReferenceModule) && module.getIndex() != parent.getModule() && parent.isExportDataRelatedMods()))) {
					
					    try {
    					    exportData(module.getIndex());
    					    
    					    // get the XML
    					    file = new File(parent.getPath(), module.getTableName() + ".xml");
    					    if (file.exists() && !canceled) {
    					        byte[] data = CoreUtilities.readFile(file);
    					        zf.addEntry(module.getTableName() + ".xml", data);
    					        
    					        // get the images
    					        File imgPath = new File(parent.getPath(), module.getTableName() + "_images");
    					        if (imgPath.exists()) {
    					            File imgFile;
    					            byte[] img;
    					            for (String image : imgPath.list()) {
    					                
    					                if (canceled) break;
    					                
    					                // add the image
    					                imgFile = new File(imgPath.toString(), image);
    					                img = CoreUtilities.readFile(imgFile);
    					                zf.addEntry(module.getTableName() + "_" + image, img);
    					                imgFile.delete();
    					            }
    					            imgPath.delete();
    					        }
    					        new File(parent.getPath(), module.getTableName() + ".xsd").delete();
    					        file.delete();
    					    }
					    } catch (Exception e) {
					        client.notifyError(e);
					    }
					}
					client.notify(DcResources.getText("msgExportedModule", module.getLabel()));
					client.notifyProcessed();
				}
				
				zf.close();
			} catch (Exception e) {
				client.notifyError(e);
			}
			
			client.notifyTaskCompleted(true, null);
			client = null;
			parent = null;
		}

		@Override
        public void notify(String message) {
		    client.notify(message);
		}

		@Override
        public void notifyProcessed() {
		    client.notifySubProcessed();
		}

		@Override
        public void notifyTaskStarted(int count) {
		    client.notifyStartedSubProcess(count);
		}

		private void exportData(int module) throws Exception {
		    
			SecuredUser su = DcConfig.getInstance().getConnector().getUser();
            List<String> items = new ArrayList<String>();
            for (String item : DataManager.getInstance().getKeys(su, new DataFilter(module)).keySet()) 
                items.add(item);
            
            if (items.size() == 0)
                return;
		    
			XmlExporter itemExporter = 
			        new XmlExporter(module, XmlExporter._MODE_NON_THREADED, module != DcModules._CONTAINER);
			
			ItemExporterSettings settings = new ItemExporterSettings();
			settings.set(ItemExporterSettings._COPY_IMAGES, Boolean.TRUE);
			settings.set(ItemExporterSettings._ALLOWRELATIVEIMAGEPATHS, Boolean.TRUE);
			settings.set(ItemExporterSettings._SCALE_IMAGES, Boolean.FALSE);
			itemExporter.setSettings(settings);
			
			itemExporter.setFile(new File(parent.getPath(), DcModules.get(module).getTableName() + ".xml"));
			
			itemExporter.setItems(items);
			itemExporter.setClient(this);
			itemExporter.start();
		}

		@Override
		public void notifyWarning(String msg) {
			notify(msg);
		}

		@Override
		public void notifyError(Throwable t) {
			client.notifyError(t);
		}

		@Override
		public boolean isCancelled() {
			return canceled;
		}
	}
}
