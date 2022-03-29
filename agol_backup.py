"""
agol_backup.py
Author: David Bucklin
Created on: 2021-01-26
Version: ArcGIS Pro / Python 3.x

This script contains functions for copying/archiving feature services from ArcGIS online. The processes are imported
by a python toolbox for use in ArcGIS Pro, but can also be used interactively in this script.

Tools include:
- GetFeatServAll: Copies one or more layers from a feature service to feature classes in a geodatabase.
- ArchiveServices: Archives feature services listed as URLs in a text file, meant to be used to regularly to maintain
   backups (one-per-day and one-per-month). Includes processes to delete backups older than specified limits.
- ServToBkp: Compares a source feature service layer to an existing backup features service layer or feature class.
   Using a date created field to find rows not present in the backup, copies the new rows from the source to the backup.

NOTE: During download, some tables throw an ugly WARNING (starting with `syntax error, unexpected WORD_WORD, expecting
SCAN_ATTR or SCAN_DATASET or SCAN_ERROR...`). This syntax error doesn't appear to affect the export of the data.
"""
import os
import arcpy
import datetime
from datetime import timedelta
import getpass


def loginAGOL(user, portal=None):
   """
   Log in to an ArcGIS online portal (e.g. ArcGIS Online), if not already. Will prompt for password.
   :param user: Username for portal
   :param portal: (optional) Portal webpage. Generally should not be used, as arcpy.GetActivePortalURL() will pull
   default portal (e.g. ArcGIS online).
   :return:
   """
   if not portal:
      portal = arcpy.GetActivePortalURL()
   ct = 0
   while arcpy.GetPortalInfo()['organization'] == '' and ct < 5:
      print('Signing in...')
      try:
         arcpy.SignInToPortal(portal, user, getpass.getpass("Password: ", False))
      except:
         ct += 1
         print('Incorrect password, ' + str(5 - ct) + ' tries left.')
      else:
         print('Signed in to ' + arcpy.GetPortalInfo()['organization'] + '.')
   if arcpy.GetPortalInfo()['organization'] == '':
      raise ValueError("Log-in failed.")
   return arcpy.GetPortalInfo()['organization']


def fieldMappings(lyr, oid_agol=True):
   """
   This function builds a default field mapping, with an additional OBJECTID_AGOL field, which maps from the OBJECTID
   of the AGOL layer. Inserted this because it seems to fix an error where the resulting FC has no fields.
   :param lyr: Layer to get field mappings for
   :param oid_agol: Whether to add a OBJECTID_AGOL field (mapping from the AGOL layer's OBJECTID).
   :return: field mappings
   """
   fms = arcpy.FieldMappings()
   for f in arcpy.ListFields(lyr):
      if f.name.upper() not in ['OBJECTID', 'SHAPE']:
         fm = arcpy.FieldMap()
         fm.addInputField(lyr, f.name)
         fms.addFieldMap(fm)
   # add OBJECTID field
   if oid_agol:
      fm = arcpy.FieldMap()
      oid = [f.name for f in arcpy.ListFields(lyr) if f.name.upper() == 'OBJECTID']
      fm.addInputField(lyr, oid[0])
      f_name = fm.outputField
      f_name.name = 'OBJECTID_AGOL'
      f_name.aliasName = 'OBJECTID_AGOL'
      f_name.type = 'Long'
      fm.outputField = f_name
      fms.addFieldMap(fm)
   return fms


def GetFeatServAll(url, gdb, fc, oid_agol=True):
   fcout = gdb + os.sep + fc
   d = arcpy.Describe(url)
   if d.datatype == 'Table':
      lyr = arcpy.MakeTableView_management(url)
      ctall = int(arcpy.GetCount_management(lyr)[0])
      arcpy.AddMessage('Getting ' + str(ctall) + ' rows...')
      fms = fieldMappings(lyr, oid_agol)
      arcpy.TableToTable_conversion(url, gdb, fc, field_mapping=fms.exportToString())
   else:
      lyr = arcpy.MakeFeatureLayer_management(url)
      ctall = int(arcpy.GetCount_management(lyr)[0])
      arcpy.AddMessage('Getting ' + str(ctall) + ' features...')
      fms = fieldMappings(lyr, oid_agol)
      arcpy.FeatureClassToFeatureClass_conversion(url, gdb, fc, field_mapping=fms.exportToString())
   ctupd = int(arcpy.GetCount_management(fcout)[0])
   e = 1
   while ctupd < ctall:
      oidmax = max([o for o in arcpy.da.SearchCursor(fcout, "OBJECTID_AGOL")])[0]
      try:
         arcpy.Append_management(url, fcout, expression="OBJECTID > " + str(oidmax), schema_type="NO_TEST", field_mapping=fms.exportToString())
      except:
         e += 1
         if e > 5:
            arcpy.AddMessage('Too many errors, giving up. Data only partially downloaded.')
            break
         else:
            arcpy.AddMessage('Server or append error, will keep trying (' + str(e) + ' of 5)...')
      ctupd = int(arcpy.GetCount_management(fcout)[0])
      arcpy.AddMessage("Done with " + str(ctupd) + " rows.")
   return fcout


def ArchiveServices(url_file, backup_folder, old_daily=10, old_monthly=12):
   """
   Archive all feature services in a text file list to a backup folder. Meant to be run on a regular basis (scheduled).
   :param url_file: Text file containing URLs of feature services (one per line)
   :param backup_folder: Folder where backups are kept. Will create new geodatabases here if they don't exists already.
   :param old_daily: Age (in days), where daily backups older than this are deleted.
   :param old_monthly: Age (in months), where monthly backups older than this are deleted.
   :return:
   """
   # get date info
   today = datetime.datetime.now()
   past = today - timedelta(days=old_daily)
   dt = today.strftime('%Y%m%d')
   old = past.strftime('%Y%m%d')

   # months
   dm = today.strftime('%Y%m')
   past = today - timedelta(days=old_monthly*31)
   oldm = past.strftime('%Y%m')

   # read urls
   file = open(url_file)
   urls0 = file.read().splitlines()
   urls = [u for u in urls0 if u[0] != '#']
   file.close()

   arcpy.env.maintainAttachments = False  # get relate tables also?
   arcpy.env.overwriteOutput = True

   if arcpy.GetPortalInfo()['organization'] != '':
      # loop over urls
      for f in urls:
         print(f)
         n = 0
         fu = str(f) + '/' + str(n)
         nm_gdb = fu.split("/")[-3]

         # set up workspace
         gdb = backup_folder + os.sep + nm_gdb + '.gdb'
         if not arcpy.Exists(gdb):
            arcpy.AddMessage('Creating geodatabase `' + gdb + '`.')
            arcpy.CreateFileGDB_management(os.path.dirname(gdb), os.path.basename(gdb))  # create if doesn't exist
         else:
            arcpy.AddMessage('Using existing geodatabase `' + gdb + '`.')
         # Need to set workspace, to list feature classes later on.
         arcpy.env.workspace = gdb

         # find layer(s) in the feature service
         chld = arcpy.da.Describe(f)['children']
         for ch in chld:
            fu = f + os.sep + ch['file']  # this creates the URL (base url + index number).
            lnm = 'L' + ch['file']
            nm = ch['name'][len(lnm):]
            arcpy.AddMessage('Copying service layer: ' + nm)
            try:
               archm = nm + '_' + dm
               archd = nm + '_' + dt
               GetFeatServAll(fu, gdb, archm)
               if ch['dataType'] == 'Table':
                  arcpy.TableToTable_conversion(archm, gdb, archd)
               else:
                  arcpy.FeatureClassToFeatureClass_conversion(archm, gdb, archd)
               arcpy.AddMessage('Successfully downloaded service: ' + nm)
            except:
               arcpy.AddMessage('Failed downloading service: ' + nm)
         # Delete old files
         try:
            # delete old
            ls = arcpy.ListFeatureClasses() + arcpy.ListTables()
            mon = [l for l in ls if l[-7] == '_']
            day = [l for l in ls if l[-9] == '_' and l[-7] != '_']
            rmdt = [i for i in mon if i[-6:] < oldm] + [i for i in day if i[-8:] < old]
            if len(rmdt) > 0:
               arcpy.AddMessage('Deleting ' + str(len(rmdt)) + ' old files...')
               arcpy.Delete_management(rmdt)
               arcpy.AddMessage('Deleted old daily archives in GDB: ' + nm_gdb)
         except:
            arcpy.AddMessage('Could not delete old daily archives in GDB: ' + nm_gdb)
   else:
      arcpy.AddMessage('No ArcGIS Online connection, are you logged in?')
   print('Done')
   return True


def ServToBkp(from_data, to_data, created_date_field="created_date"):
   """
   Using a 'created_date' field to find new records, update a copy of a feature service layer (either another service
    layer or a local feature class), by appending new data from the feature service layer.
   :param from_data: Url of feature service / class to copy from
   :param to_data: Url of feature service / class to copy to
   :param created_date_field: Field name of date field used to identify new rows
   :return: url_to
   """
   scr = arcpy.env.scratchGDB

   # Comparisons
   d1 = arcpy.Describe(from_data)
   d2 = arcpy.Describe(to_data)
   # coulddo: move to pre-fn checkes in toolbox
   if d1.datatype != d2.datatype:
      raise ValueError("Datasets are not the same data type.")
   if d1.shapeType != d2.shapeType:
      raise ValueError("Datasets are not the same feature type.")
   d1_fld = [a.name for a in arcpy.ListFields(from_data) if a.type != "OID" and a.name not in ['globalid', "Shape"]]
   d2_fld = [a.name for a in arcpy.ListFields(to_data)]
   for i in d1_fld:
      if i not in d2_fld:
         raise ValueError("Field " + i + " is not found in the destination dataset.")
   # Get maximium date from d2, add one second.
   last_date = max([a[0] for a in arcpy.da.SearchCursor(to_data, created_date_field)])
   last_date2 = last_date + timedelta(seconds=1)
   query = created_date_field + " >= timestamp '" + last_date2.strftime("%Y-%m-%d %H:%M:%S") + "'"
   # Make copy of new data
   tmp = scr + os.sep + "tmp_append"
   with arcpy.EnvManager(overwriteOutput=True):
      arcpy.FeatureClassToFeatureClass_conversion(from_data, scr, "tmp_append", where_clause=query)
   ct = arcpy.GetCount_management(tmp).getOutput(0)
   if ct == '0':
      arcpy.AddMessage("No new data to append.")
   else:
      arcpy.AddMessage("Appending " + ct + " new rows...")
      arcpy.Append_management(scr + os.sep + 'tmp_append', to_data, 'NO_TEST')
   arcpy.AddMessage("Finished.")
   return to_data


def main():

   ## Example backup procedure. This could be scheduled to run on a daily basis (e.g with Windows Task Scheduler).

   # portal = loginAGOL('username')
   # backup_folder = r"D:\backup_folder"
   # url_file = 'urls.txt'
   # ArchiveServices(url_file, backup_folder, old_daily=10, old_monthly=12)


if __name__ == '__main__':
   main()
