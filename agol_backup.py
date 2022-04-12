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

NOTE: During download, some tables throw an ugly WARNING, starting with `syntax error, unexpected WORD_WORD, expecting
SCAN_ATTR or SCAN_DATASET or SCAN_ERROR...`. This syntax error doesn't appear to affect the export of the data.
"""
import os
import arcpy
import datetime
from datetime import timedelta
import getpass
import pandas as pd
import time
import numpy


def loginAGOL(user, portal=None):
   """
   Check if logged in, and if not, log in to an ArcGIS online portal (e.g. ArcGIS Online). Will prompt for password.
   :param user: Username for portal
   :param portal: (optional) Portal webpage. Generally should not be used, as arcpy.GetActivePortalURL() will pull
   the default portal (i.e. ArcGIS online).
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
   Builds a default field mapping, with an additional OBJECTID_AGOL field, which maps from the OBJECTID
   of the AGOL layer. Added this because it seemed to fix an error where the resulting FC has no fields.
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


def GetFeatServAll(url, gdb, fc, oid_agol=True, query=None):
   """Copy a feature service layer from ArcGIS Online to feature class.
   :param url: Url of the feature service layer (including index number) to download
   :param gdb: Output geodatabase
   :param fc: Output feature class
   :param oid_agol: Whether to copy OBJECTID from AGOL to a new attribute, 'OBJECTID_AGOL'.
   :return: feature class
   """
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
   dt = today.strftime('%Y%m%d')
   dm = today.strftime('%Y%m')

   # old daily
   past = today - timedelta(days=old_daily)
   oldd = past.strftime('%Y%m%d')
   # old monthly
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
            arcpy.AddMessage('Copying layer: ' + nm)
            try:
               archm = nm + '_' + dm
               archd = nm + '_' + dt
               GetFeatServAll(fu, gdb, archm)
               if ch['dataType'] == 'Table':
                  arcpy.TableToTable_conversion(archm, gdb, archd)
               else:
                  arcpy.FeatureClassToFeatureClass_conversion(archm, gdb, archd)
               arcpy.AddMessage('Successfully downloaded layer: ' + nm)
            except:
               arcpy.AddMessage('Failed downloading layer: ' + nm)
         # Delete old files
         try:
            # delete old
            ls = arcpy.ListFeatureClasses() + arcpy.ListTables()
            mon = [l for l in ls if l[-7] == '_']
            day = [l for l in ls if l[-9] == '_' and l[-7] != '_']
            rmdt = [i for i in mon if i[-6:] < oldm] + [i for i in day if i[-8:] < oldd]
            if len(rmdt) > 0:
               arcpy.AddMessage('Deleting ' + str(len(rmdt)) + ' old files...')
               arcpy.Delete_management(rmdt)
               arcpy.AddMessage('Deleted old daily archives in GDB: ' + nm_gdb)
         except:
            arcpy.AddMessage('Failed to delete old daily archives in GDB: ' + nm_gdb)
   else:
      arcpy.AddMessage('No ArcGIS Online connection, are you logged in?')
   print('Done')
   return True


def ServToBkp(from_data, to_data, created_date_field="created_date", append_data=None, append=True):
   """
   Using a 'created_date' field to find new records, update a copy of a feature service layer (either another service
    layer or a local feature class), by appending new data from the feature service layer.
   :param from_data: Url of feature service / class to copy from
   :param to_data: Url of feature service / class to copy to
   :param created_date_field: Field name of date field used to identify new rows
   :param append_data: Name of feature class holding new append data. Use if you want to use/save this data separately.
   :param append: Whether to append data to to_data, or just return new data.
      If you need to alter data before the append, this should be used with append=False.
   :return: url_to
   """
   if append_data is None:
      append_data = arcpy.env.scratchGDB + os.sep + 'tmp_append'
   # Comparisons
   d1 = arcpy.Describe(from_data)
   d2 = arcpy.Describe(to_data)
   if d1.datatype != d2.datatype:
      msg = "Datasets are not the same data type (" + d1.datatype + ", " + d2.datatype + ")."
      raise ValueError(msg)
   if d1.shapeType != d2.shapeType:
      msg = "Datasets are not the same shape type (" + d1.shapeType + ", " + d2.shapeType + ")."
      raise ValueError(msg)
   # Check field information
   d1_fld = [a.name for a in arcpy.ListFields(from_data) if a.type != "OID" and a.name not in ['globalid', "Shape"]]
   d2_fld = [a.name for a in arcpy.ListFields(to_data) if a.type != "OID" and a.name not in ['globalid', "Shape"]]
   # Unmatched fields from both FCs
   d1_miss = [a for a in d1_fld if a not in d2_fld]
   d2_miss = [a for a in d2_fld if a not in d1_fld]
   # Find fields in `from_data` which have an underscore added in `to_data`. so they can be sent to the correct field.
   # This can happen because some field names are reserved in AGOL.
   repl = []
   for d in d1_miss:
      if d + '_' in d2_miss:
         print('Will send values from `' + d + '` to field `' + d + '_`.')
         repl.append([d, d + '_'])
   # Get maximium date from d2, add one second.
   if arcpy.GetCount_management(to_data)[0] == '0':
      print("No data exists in the copy dataset")
      last_date2 = datetime.datetime.today() - timedelta(days=365)
   else:
      last_date = max([a[0] for a in arcpy.da.SearchCursor(to_data, created_date_field)])
      last_date2 = last_date + timedelta(seconds=1)
   query = created_date_field + " >= timestamp '" + last_date2.strftime("%Y-%m-%d %H:%M:%S") + "'"
   # Copy new data from the feature service
   with arcpy.EnvManager(overwriteOutput=True):
      lyr = arcpy.MakeFeatureLayer_management(from_data)
      fms = fieldMappings(lyr)  # Adds the OBJECTID_AGOL field, mapping from original OBJECTID
      arcpy.FeatureClassToFeatureClass_conversion(from_data, os.path.dirname(append_data), os.path.basename(append_data), query, fms)
   ct = arcpy.GetCount_management(append_data).getOutput(0)
   if ct == '0':
      arcpy.AddMessage("No new data to append.")
   else:
      for r in repl:
         arcpy.AlterField_management(append_data, r[0], r[1], clear_field_alias=False)
      if append:
         arcpy.AddMessage("Appending " + ct + " new rows...")
         arcpy.Append_management(append_data, to_data, 'NO_TEST')
      else:
         print("append=False, not appending data.")
   arcpy.AddMessage("Finished.")
   return to_data


def JoinFast(ToTab, ToFld, FromTab, FromFld, JoinFlds):
   """
   An alternative to arcpy's JoinField_management for table joins.
   Uses python dictionary and Search/Update cursors.
   Tested about 50x faster than arcpy.JoinFields_management for a 500k-row, one-to-one join.
   Adapted from: https://gis.stackexchange.com/questions/207943/speeding-up-join-in-arcpy

   Note that unlike the arcpy function, this will overwrite existing fields in ToTab with names matching JoinFlds (arcpy
   will keep both, renaming the new fields).

   :param ToTab = The table to which fields will be added
   :param ToFld = The key field in ToTab, used to match records in FromTab
   :param FromTab = The table from which fields will be copied
   :param FromFld = the key field in FromTab, used to match records in ToTab
   :param JoinFlds = the list of fields to be added
   """
   if type(JoinFlds) != list:
      JoinFlds = [JoinFlds]
   flds_info = [a for a in arcpy.ListFields(FromTab) if a.name in JoinFlds]
   if len(flds_info) == 0:
      print('No fields found, no changes made.')
      return ToTab
   else:
      flds = [f.name for f in flds_info]
      print('Joining [' + ', '.join(flds) + ']...')
   r = list(range(1, len(JoinFlds) + 1))
   joindict = {}
   with arcpy.da.SearchCursor(FromTab, [FromFld] + flds) as rows:
      for row in rows:
         joinval = row[0]
         joindict[joinval] = [row[a] for a in r]
   del row, rows
   tFlds = [a.name for a in arcpy.ListFields(ToTab)]
   # Add fields
   for j in JoinFlds:
      ft = [a.type for a in flds_info if a.name == j][0]
      if j in tFlds:
         arcpy.DeleteField_management(ToTab, j)
      if ft == 'String':
         arcpy.AddField_management(ToTab, j, "TEXT", field_length=8000)
      elif ft == 'Integer':
         arcpy.AddField_management(ToTab, j, "LONG")
      elif ft == 'Date':
         arcpy.AddField_management(ToTab, j, "DATE")
      else:
         arcpy.AddField_management(ToTab, j, "DOUBLE")
   # Do updates
   with arcpy.da.UpdateCursor(ToTab, [ToFld] + flds) as recs:
      for rec in recs:
         keyval = rec[0]
         if keyval in joindict:
            for a in r:
               rec[a] = joindict[keyval][a - 1]
            recs.updateRow(rec)
   del rec, recs
   return ToTab


def arcgis_table_to_df(in_fc, input_fields=None, query=""):
   """Function will convert an arcgis table into a pandas dataframe with an object ID index, and the selected
   input fields using an arcpy.da.SearchCursor.
   :param - in_fc - input feature class or table to convert
   :param - input_fields - fields to input to a da search cursor for retrieval
   :param - query - sql query to grab appropriate values
   :returns - pandas.DataFrame
   """
   OIDFieldName = arcpy.Describe(in_fc).OIDFieldName
   if input_fields:
      final_fields = [OIDFieldName] + input_fields
   else:
      final_fields = [field.name for field in arcpy.ListFields(in_fc)]
   data = [row for row in arcpy.da.SearchCursor(in_fc, final_fields, where_clause=query)]
   fc_dataframe = pd.DataFrame(data, columns=final_fields)
   fc_dataframe = fc_dataframe.set_index(OIDFieldName, drop=True)
   return fc_dataframe


def prep_track_pts(in_pts, by_session=True, break_tracks_seconds=600):
   """
   Function to prepare tracking points for making track lines. Includes assignment of a 'use' column indicating points
   to use in track lines (i.e. points will acceptable accuracy), as well as a unique track line ID, either by session
   or user.
   :param in_pts: Input track points
   :param by_session: If True, will use 'session_id' to assign unique track line ID. If False, 'full_name' is used.
   :param break_tracks_seconds: duration in seconds, where breaks collection of points greater than this will result
   in a new track line ID (regardless of session or user). Default = 600 = 10 minutes.
   :return:
   """
   print("Prepping track points...")
   flds = ['use', 'unique_track_id']
   arcpy.DeleteField_management(in_pts, flds)
   fn = '''def pt_use(horiz, speed, course):
      if horiz <= 10:
         return 1
      if horiz <= 25 and (speed >= 0 or course >= 0):
         return 1
      if speed >= 0 and course >= 0:
         return 1
      return 0
   '''
   arcpy.CalculateField_management(in_pts, 'use', 'pt_use(!horizontal_accuracy!, !speed!, !course!)', code_block=fn,
                                   field_type="SHORT")
   # Get data frame of points. Since this is to define continuous tracking, it seems better to use all points (not just use = 1).
   if by_session:
      break_by = 'session_id'
   else:
      break_by = 'full_name'
   df = arcgis_table_to_df(in_pts, input_fields=[break_by, 'location_timestamp'])  # , query="use = 1")
   df['location_timestamp'] = pd.to_datetime(df["location_timestamp"])
   df = df.sort_values([break_by, 'location_timestamp'])

   # diff is time in seconds between the current row and previous row
   df["diff"] = (df['location_timestamp'] - df['location_timestamp'].shift(periods=1)).dt.seconds

   # update diff for new session or user (so always assigned to new track)
   df.loc[df[break_by] != (df[break_by].shift(periods=1)), "diff"] = break_tracks_seconds + 1
   # switch indicates start of new track_id
   df['switch'] = numpy.where(df['diff'] > break_tracks_seconds, 1, 0)
   df['track_id'] = df['switch'].cumsum()
   df.to_csv('tmp_tracks.csv')
   JoinFast(in_pts, 'OBJECTID', 'tmp_tracks.csv', 'OBJECTID', ['track_id'])
   print('Done.')
   return in_pts


def make_track_lines(in_pts, out_track_lines):
   """
   :param in_pts: Input track points, after run through prep_track_pts
   :param out_track_lines: Output track lines
   :return: out_track_lines
   See: https://doc.arcgis.com/en/tracker/help/use-tracks.htm, which describes the default Track_Lines created on
   ArcGIS online. This process builds a lines similar to the Track_Lines dataset, but generates lines only for use=1
   points (identified in prep_track_pts), and a select set of attributes (see line_flds).
   """
   print("Making track lines from use = 1 points...")
   # Make track lines from use = 1 points.
   lyr = arcpy.MakeFeatureLayer_management(in_pts, where_clause='use = 1')
   arcpy.PointsToLine_management(lyr, out_track_lines, 'track_id', 'location_timestamp')
   # Summarize tracks
   line_flds = [['full_name', 'FIRST', 'full_name'], ['location_timestamp', 'MIN', 'start_time'], ['location_timestamp', 'MAX', 'end_time'],
                ['altitude', 'MIN', 'min_altitude'], ['altitude', 'MAX', 'max_altitude'], ['altitude', 'MEAN', 'avg_altitude'],
                # ['horizontal_accuracy', 'MIN', 'min_horizontal_accuracy'], ['horizontal_accuracy', 'MAX', 'max_horizontal_accuracy'],
                ['horizontal_accuracy', 'MEAN', 'avg_horizontal_accuracy'],
                # ['vertical_accuracy', 'MIN', 'min_vertical_accuracy'], ['vertical_accuracy', 'MAX', 'max_vertical_accuracy'],
                ['vertical_accuracy', 'MEAN', 'avg_vertical_accuracy'],
                # ['speed', 'MIN', 'min_speed'], ['speed', 'MAX', 'max_speed'],
                ['speed', 'MEAN', 'avg_speed'],
                # ['battery_percentage', 'MIN', 'min_battery_percentage'], ['battery_percentage', 'MAX', 'max_battery_percentage']
                ['session_id', 'FIRST', 'session_id'], ['created_date', 'MAX', 'created_date']
                ]
   arcpy.Statistics_analysis(lyr, 'tmp_track_stats', [a[0:2] for a in line_flds], case_field="track_id")
   # Rename fields
   for f in line_flds:
      arcpy.AlterField_management('tmp_track_stats', f[1] + '_' + f[0], f[2], clear_field_alias=True)
   arcpy.AlterField_management('tmp_track_stats', 'FREQUENCY', 'count_')  # note: 'count' is reserved on AGOL, that is why 'count_' is used.
   # Calculate duration and total distance
   dur = 'round((!end_time!.timestamp() - !start_time!.timestamp()) / 60, 2)'
   arcpy.CalculateField_management('tmp_track_stats', 'duration_minutes', dur, field_type="FLOAT")
   flds = [a.name for a in arcpy.ListFields('tmp_track_stats') if not a.name in ['OBJECTID', 'track_id']]
   JoinFast(out_track_lines, 'track_id', 'tmp_track_stats', 'track_id', flds)
   arcpy.CalculateGeometryAttributes_management(out_track_lines, "distance_covered_meters LENGTH_GEODESIC", "METERS")
   return out_track_lines



def main():
   """
   Example archive procedure. This script be scheduled to run on a daily basis (e.g with Windows Task Scheduler), by
   executing a '.bat' file with the following command:
   "C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\propy.bat" "C:\path_to\AGOLBackupTools\agol_backup.py"
   """
   # portal = loginAGOL('username')
   # backup_folder = r"C:\path_to\backup_folder"
   # url_file = r'C:\path_to\urls.txt'
   # ArchiveServices(url_file, backup_folder, old_daily=10, old_monthly=12)

   """
   Example 'service to backup service' procedure. This is needed for location service feature services, which 
   automatically delete data older than 30 days.
   """
   # from_data = 'https://locationservices1.arcgis.com/PxUNqSbaWFvFgHnJ/arcgis/rest/services/a2c3527390c849d78e8d038345e4f7af_Track_View/FeatureServer/2'
   # to_data = 'https://services1.arcgis.com/PxUNqSbaWFvFgHnJ/arcgis/rest/services/DNH_Stewardship_Tracks_2022/FeatureServer/0'
   # ServToBkp(from_data, to_data, created_date_field="created_date")

   return


if __name__ == '__main__':
   main()
