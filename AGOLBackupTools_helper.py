"""
AGOLBackupTools_helper.py
Author: David Bucklin
Created on: 2021-01-26
Version: ArcGIS Pro / Python 3.x

This script contains functions for copying/archiving feature services from ArcGIS online. Tools for handling location
 tracking data are also included. The processes are imported by a python toolbox for use in ArcGIS Pro, but can also 
 be used interactively in this script, which could be set up to run on a schedule (e.g. with Windows Task Scheduler).

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
import base64

def makeCredFile(file, username, password):
   """
   Make a new text file to store credentials
   :param file: File path
   :param username: AGOL username
   :param password: AGOL password. Will store an encoded version of password.
   :return: 
   """
   string_bytes = password.encode("ascii")
   base64_bytes = base64.b64encode(string_bytes)
   base64_string = base64_bytes.decode("ascii")
   if os.path.exists(file):
      os.remove(file)
   with open(file, 'w') as f:
      f.write(username)
      f.write("\n")
      f.write(base64_string)
   print("Created credentials file " + file + ".")
   return file

def loginAGOL(user=None, credentials=None, portal=None):
   """
   Check if logged in, and if not, use username or credentials file to log in to an ArcGIS online portal
   (e.g. ArcGIS Online). If you provide user only, will prompt for password.
   :param user: Username for portal. Will be used (including prompt for password) if credentials are not supplied.
   :param credentials: Credentials file, Text file with two lines: (1) username and (2) encoded password (see makeCredFile)
   :param portal: (optional) Portal webpage. Generally should not be used, as arcpy.GetActivePortalURL() will pull
   the default portal (i.e. ArcGIS online).
   :return: connection information

   Note: username is case-sensitive (even though logging in to AGOL a web browser is not!)
   """
   if not portal:
      portal = arcpy.GetActivePortalURL()
   ct = 0
   if credentials:
      with open(credentials) as f:
         cred = [line.splitlines()[0] for line in f]
      print('Using credentials file to connect to `' + portal + '` with username `' + cred[0] + '`...')
      arcpy.SignInToPortal(portal, cred[0], password=base64.b64decode(cred[1]).decode("utf-8"))
   while arcpy.GetPortalInfo()['organization'] == '' and ct < 2:
      if user:
         print('Signing in with username `' + user + '`...')
         try:
            arcpy.SignInToPortal(portal, user, getpass.getpass("Password: ", False))
         except:
            ct += 1
            print('Incorrect password, ' + str(2 - ct) + ' tries left.')
      else:
         print("Need to provide username or credentials file.")
         ct = 2
   if arcpy.GetPortalInfo()['organization'] == '':
      raise ValueError("Log-in failed.")
   else:
      print('Signed in to ' + arcpy.GetPortalInfo()['organization'] + '.')
   return arcpy.GetPortalInfo()['organization']

def fieldMappings(lyr, oid_agol=True):
   """
   Builds a default field mapping, with an optional OBJECTID_AGOL field, which maps from the OID field
   of the AGOL layer. Added this because it seemed to fix an error where the resulting FC has no fields.
   :param lyr: Layer to get field mappings for
   :param oid_agol: Whether to add a OBJECTID_AGOL field (mapping from the AGOL layer's OID field).
   :return: field mappings
   """
   fms = arcpy.FieldMappings()
   oid_name = [f.name for f in arcpy.ListFields(lyr) if f.type == 'OID'][0]
   for f in arcpy.ListFields(lyr):
      if f.name.upper() not in [oid_name.upper(), 'SHAPE']:
         fm = arcpy.FieldMap()
         fm.addInputField(lyr, f.name)
         fms.addFieldMap(fm)
   # map OID field to output OBJECTID_AGOL field
   if oid_agol:
      fm = arcpy.FieldMap()
      fm.addInputField(lyr, oid_name)
      f_name = fm.outputField
      f_name.name = 'OBJECTID_AGOL'
      f_name.aliasName = 'OBJECTID_AGOL'
      f_name.type = 'Long'
      fm.outputField = f_name
      fms.addFieldMap(fm)
   return fms


def GetFeatServAll(service, gdb, fc, oid_agol=True):
   """Copy a feature service layer from ArcGIS Online to feature class.
   :param service: Feature service layer (if URL, include index number) to download
   :param gdb: Output geodatabase
   :param fc: Output feature class
   :param oid_agol: Whether to copy OBJECTID from AGOL to a new attribute, 'OBJECTID_AGOL'.
   :return: feature class
   """
   fcout = gdb + os.sep + fc
   d = arcpy.Describe(service)
   oid_name = [f.name for f in arcpy.ListFields(service) if f.type == 'OID'][0]
   if d.datatype == 'Table':
      lyr = arcpy.MakeTableView_management(service)
      ctall = int(arcpy.GetCount_management(lyr)[0])
      arcpy.AddMessage('Getting ' + str(ctall) + ' rows...')
      fms = fieldMappings(lyr, oid_agol)
      arcpy.ExportTable_conversion(service, fcout, field_mapping=fms.exportToString())
      # arcpy.TableToTable_conversion(service, gdb, fc, field_mapping=fms.exportToString())
   else:
      lyr = arcpy.MakeFeatureLayer_management(service)
      ctall = int(arcpy.GetCount_management(lyr)[0])
      arcpy.AddMessage('Getting ' + str(ctall) + ' features...')
      fms = fieldMappings(lyr, oid_agol)
      # arcpy.FeatureClassToFeatureClass_conversion(service, gdb, fc, field_mapping=fms.exportToString())
      oid_name = [f.name for f in arcpy.ListFields(lyr) if f.type == 'OID'][0]
      arcpy.ExportFeatures_conversion(service, fcout, field_mapping=fms.exportToString())
   ctupd = int(arcpy.GetCount_management(fcout)[0])
   e = 1
   while ctupd < ctall:
      oidmax = max([o for o in arcpy.da.SearchCursor(fcout, "OBJECTID_AGOL")])[0]
      ctupd0 = int(arcpy.GetCount_management(fcout)[0])
      try:
         arcpy.Append_management(service, fcout, expression=oid_name + " > " + str(oidmax), schema_type="NO_TEST", field_mapping=fms.exportToString())
      except:
         e += 1
         if e > 5:
            arcpy.AddMessage('Too many errors, giving up. Data only partially downloaded.')
            break
         else:
            arcpy.AddMessage('Server or append error, will keep trying (' + str(e) + ' of 5)...')
      else:
         # If no append error, check if records were actually added. If not, exit loop.
         ctupd = int(arcpy.GetCount_management(fcout)[0])
         arcpy.AddMessage("Done with " + str(ctupd) + " rows.")
         if ctupd0 == ctupd:
            break
   if ctupd != ctall:
      arcpy.AddWarning("Number of records downloaded (" + str(ctupd) +
                       ") did not match original count of records in the feature service layer (" + str(ctall) + ")."
                       "\nThis could happen due to download errors, or if the service layer is actively being updated.")
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
   urls = [u.replace(" ", "") for u in urls0 if u[0] != '#']  # correct URLs with extra space at end.
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

         # find layer(s) in the feature service
         try:
            chld = arcpy.da.Describe(f)['children']
         except:
            print("Error trying to access: `" + f + "`.")
            continue
         for ch in chld:
            fu = f + os.sep + ch['file']  # this creates the URL (base url + index number).
            lnm = 'L' + ch['file']
            nm = ch['name'][len(lnm):]
            arcpy.AddMessage('Copying layer: ' + nm)
            try:
               archm = nm + '_' + dm
               archd = nm + '_' + dt
               GetFeatServAll(fu, gdb, archm)
               # Make daily copy
               arcpy.Copy_management(gdb + os.sep + archm, gdb + os.sep + archd)
               arcpy.AddMessage('Successfully downloaded layer: ' + nm)
            except:
               arcpy.AddMessage('Failed downloading layer: ' + nm)
         # Delete old files
         try:
            # list all FC and Tables
            with arcpy.EnvManager(workspace=gdb):
               ls = arcpy.ListFeatureClasses() + arcpy.ListTables()
            mon = [l for l in ls if l[-7] == '_']
            day = [l for l in ls if l[-9] == '_' and l[-7] != '_']
            rmdt = [i for i in mon if i[-6:] < oldm] + [i for i in day if i[-8:] < oldd]
            if len(rmdt) > 0:
               todel = [gdb + os.sep + i for i in rmdt]
               arcpy.AddMessage('Deleting ' + str(len(todel)) + ' old files...')
               arcpy.Delete_management(todel)
               arcpy.AddMessage('Deleted old daily archives in GDB: ' + nm_gdb)
         except:
            arcpy.AddMessage('Failed to delete old daily archives in GDB: ' + nm_gdb)
      print('Done archiving services.')
   else:
      arcpy.AddMessage('No ArcGIS Online connection, are you logged in?')
   return True


def ServToBkp(from_data, to_data, created_date_field="created_date", append_data=None):
   """
   Using a 'created_date' field to find new records, update a copy of a feature service layer (either another service
    layer or a local feature class), by appending new data from the feature service layer.
   :param from_data: Url of feature service / class to copy from
   :param to_data: Url of feature service / class to copy to
   :param created_date_field: Field name of date field which is used to identify new rows
   :param append_data: Name of new feature class to create with data being appended. By default this is temp data, but
      this can be used to save this data.
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
         arcpy.AddMessage('Will send values from `' + d + '` to field `' + d + '_`.')
         repl.append([d, d + '_'])
   # Get maximium date from d2.
   if arcpy.GetCount_management(to_data)[0] == '0':
      last_date = datetime.datetime.today() - timedelta(days=365)
      arcpy.AddMessage("No data exists in the copy dataset, will pull all data with created date after " + str(last_date) + "...")
   else:
      last_date = max([a[0] for a in arcpy.da.SearchCursor(to_data, created_date_field)])
   # Note that this query will also select records EQUAL to the last_date.
   query = created_date_field + " >= timestamp '" + last_date.strftime("%Y-%m-%d %H:%M:%S") + "'"
   # Copy new data from the feature service
   with arcpy.EnvManager(overwriteOutput=True):
      lyr = arcpy.MakeFeatureLayer_management(from_data)
      fms = fieldMappings(lyr)  # Adds the OBJECTID_AGOL field, mapping from original OBJECTID
      arcpy.ExportFeatures_conversion(from_data, append_data, query, field_mapping=fms)  # , sort_field=[[created_date_field, "ASCENDING"]])
   ct = arcpy.GetCount_management(append_data).getOutput(0)
   if ct != '0':
      arcpy.AddMessage(ct + " records pulled, checking for duplicates in backup data...")
      # This compares OBJECTID_AGOL from in backup to append_data, and deletes rows in append_data already in to_data.
      # NOTE: simple '=' in query below does not work, that is why '>=' is used.
      query = created_date_field + " >= timestamp '" + last_date.strftime("%Y-%m-%d %H:%M:%S") + "'"
      lyr_to = arcpy.MakeFeatureLayer_management(to_data, where_clause=query)
      to_oids = [a[0] for a in arcpy.da.SearchCursor(lyr_to, 'OBJECTID_AGOL')]
      with arcpy.da.UpdateCursor(append_data, 'OBJECTID_AGOL') as curs:
         for r in curs:
            if r[0] in to_oids:
               curs.deleteRow()
   # Get new count
   ct = arcpy.GetCount_management(append_data).getOutput(0)
   if ct == '0':
      arcpy.AddMessage("No new data to append.")
   else:
      for r in repl:
         arcpy.AlterField_management(append_data, r[0], r[1], clear_field_alias=False)
      if "session_id" in d2_fld:
         add_user_date(append_data)
      arcpy.AddMessage("Appending " + ct + " new rows...")
      arcpy.Append_management(append_data, to_data, 'NO_TEST')
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


def prep_track_pts(in_pts, break_by='user_date', break_tracks_seconds=600):
   """
   Function to prepare tracking points for making track lines. Includes assignment of a 'use' column indicating points
   to use in track lines (i.e. points with acceptable accuracy), as well as a unique track line ID, either by session
   or user.
   :param in_pts: Input track points
   :param break_by: Field used group points by, for creating track lines. Best options: "session_id", "full_name",
      or "user_date" (a derived field combining user name + datestamp)
   :param break_tracks_seconds: duration in seconds, where a break in collection of points greater than this will result
   in a new track line ID (regardless of session or user). Default = 600 seconds = 10 minutes.
   :return: in_pts

   Borrowed point inclusion logic from here:
   https://github.com/Esri/tracker-scripts/blob/master/notebooks/examples/Create%20Track%20Lines%20From%20Points.ipynb
   """
   arcpy.AddMessage("Prepping track points...")
   flds = ['use', 'unique_track_id']
   arcpy.DeleteField_management(in_pts, flds)
   fn = '''def pt_use(horiz, speed, course):
      if speed is None:
         speed = -1
      if course is None:
         course = -1
      if horiz <= 10:
         return 1
      elif horiz <= 25 and (speed >= 0 or course >= 0):
         return 1
      elif speed >= 0 and course >= 0:
         return 1
      else:
         return 0
   '''
   arcpy.CalculateField_management(in_pts, 'use', 'pt_use(!horizontal_accuracy!, !speed!, !course!)', code_block=fn,
                                   field_type="SHORT")
   # Assign a track_id to continuously-collected points having the same session_id or full_name.
   # Get data frame of points. Since this process is for defining continuous tracking, all points are used (even use=0).
   df = arcgis_table_to_df(in_pts, input_fields=[break_by, 'location_timestamp'])  # , query="use = 1")
   df['location_timestamp'] = pd.to_datetime(df["location_timestamp"])
   df = df.sort_values([break_by, 'location_timestamp'])

   # diff is time in seconds between the current row and previous row
   # df["diff"] = (df['location_timestamp'] - df['location_timestamp'].shift(periods=1)).dt.seconds
   df["diff"] = round((df['location_timestamp'] - df['location_timestamp'].shift(periods=1)).dt.total_seconds(), 2)

   # update diff for new session or user (so it will always be assigned to a new track)
   df.loc[df[break_by] != (df[break_by].shift(periods=1)), "diff"] = break_tracks_seconds + 1
   # switch indicates start of new track_id
   df['switch'] = numpy.where(df['diff'] > break_tracks_seconds, 1, 0)
   df['track_id'] = df['switch'].cumsum()

   # update track ids
   arcpy.AddField_management(in_pts, 'track_id', 'LONG')
   arcpy.AddField_management(in_pts, 'seconds_elapsed', 'FLOAT')
   with arcpy.da.UpdateCursor(in_pts, ['OBJECTID', 'track_id', 'seconds_elapsed']) as curs:
      for r in curs:
         dat = df[df.index == r[0]].iloc[0]
         r[1] = dat["track_id"]
         if dat["switch"] == 0:
            r[2] = dat["diff"]
         else:
            r[2] = 0
         curs.updateRow(r)
   arcpy.AddMessage('Point attributes calculated.')
   return in_pts


def make_track_lines(in_pts, out_track_lines):
   """
   :param in_pts: Input track points, processed with prep_track_pts
   :param out_track_lines: Output track lines
   :return: out_track_lines
   This process builds track lines from location tracking points for use=1 points (identified in prep_track_pts),
   with a select set of summary attributes (see line_flds).

   Also see: https://doc.arcgis.com/en/tracker/help/use-tracks.htm, which describes the auto-generated Track_Lines
   created by ArcGIS online. The result from this function is similar, but can be different for a couple of reasons:
      - 'low accuracy' points excluded by one process may have been included by the other
      - auto-generated AGOL track lines are limited to one hour in duration
   """
   arcpy.AddMessage("Making track lines from use = 1 points...")
   # Make track lines from use = 1 points.
   lyr = arcpy.MakeFeatureLayer_management(in_pts, where_clause='use = 1')
   arcpy.PointsToLine_management(lyr, out_track_lines, 'track_id', 'location_timestamp')
   # Summarize tracks (decided to exclude some fields unlikely to be used)
   line_flds = [['full_name', 'FIRST', 'full_name'], ['location_timestamp', 'MIN', 'start_time'], ['location_timestamp', 'MAX', 'end_time'],
                ['altitude', 'MIN', 'min_altitude'], ['altitude', 'MAX', 'max_altitude'], ['altitude', 'MEAN', 'avg_altitude'],
                # ['horizontal_accuracy', 'MIN', 'min_horizontal_accuracy'], ['horizontal_accuracy', 'MAX', 'max_horizontal_accuracy'],
                ['horizontal_accuracy', 'MEAN', 'avg_horizontal_accuracy'],
                # ['vertical_accuracy', 'MIN', 'min_vertical_accuracy'], ['vertical_accuracy', 'MAX', 'max_vertical_accuracy'],
                ['vertical_accuracy', 'MEAN', 'avg_vertical_accuracy'],
                # ['speed', 'MIN', 'min_speed'], ['speed', 'MAX', 'max_speed'],
                ['speed', 'MEAN', 'avg_speed'],
                # ['battery_percentage', 'MIN', 'min_battery_percentage'], ['battery_percentage', 'MAX', 'max_battery_percentage']
                ['session_id', 'FIRST', 'session_id'], ['created_date', 'MAX', 'created_date'],
                ['user_date', 'FIRST', 'user_date']]
   tmp_track_stats = arcpy.env.scratchGDB + os.sep + "tmp_track_stats"
   arcpy.Statistics_analysis(lyr, tmp_track_stats, [a[0:2] for a in line_flds], case_field="track_id")
   # Rename fields
   for f in line_flds:
      arcpy.AlterField_management(tmp_track_stats, f[1] + '_' + f[0], f[2], clear_field_alias=True)
   arcpy.AlterField_management(tmp_track_stats, 'FREQUENCY', 'count_')  # note: 'count' is a reserved field name on AGOL, that is why 'count_' is used.
   # Calculate duration and total distance
   dur = 'round((!end_time!.timestamp() - !start_time!.timestamp()) / 60, 2)'
   arcpy.CalculateField_management(tmp_track_stats, 'duration_minutes', dur, field_type="FLOAT")
   flds = [a.name for a in arcpy.ListFields(tmp_track_stats) if a.name not in ['OBJECTID', 'track_id']]
   if arcpy.GetCount_management(out_track_lines)[0] == '0':
      arcpy.AddMessage("No track lines generated, exiting.")
      return out_track_lines
   JoinFast(out_track_lines, 'track_id', tmp_track_stats, 'track_id', flds)
   arcpy.CalculateGeometryAttributes_management(out_track_lines, "distance_covered_meters LENGTH_GEODESIC", "METERS")
   return out_track_lines


def add_user_date(loc_pts):
   """
   Adds a user_date column to a track points backup. This column can be used in track line generation. Also fills in
   missing session_id with the calculated user_date.
   :param loc_pts: Copy of track points from a location tracking feature service
   :return:
   """
   utcfn = """def utc2local(utc):
       epoch = time.mktime(utc.timetuple())
       offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
       return utc + offset"""
   # Calculate the user-date combo field, which can be used for track line generation.
   # headsup: location_timestamp from AGOL is in the UTC time zone. utclocal() converts to local time so that
   #  the date is extracted in the local time zone. Note that this is the only tz-adjustment made in this module. 
   #  Otherwise, datetime fields are used as-is (UTC).
   arcpy.CalculateField_management(loc_pts, "user_date",
                                   "!full_name! + '-' + utc2local(!location_timestamp!).strftime('%Y%m%d')", 
                                   code_block=utcfn)
   # Fill in any missing session_ids
   lyr = arcpy.MakeFeatureLayer_management(loc_pts, where_clause="session_id IS NULL")
   if arcpy.GetCount_management(lyr)[0] != '0':
      print("Filling in missing session_id...")
      arcpy.CalculateField_management(lyr, "session_id", "!user_date!")
      del lyr
   return loc_pts


def main():
   """
   This section contains examples of processes that could be scheduled to run daily/weekly (e.g. with Windows Task
   Scheduler), by executing a '.bat' file with the following command:
   "C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\propy.bat" "C:\path_to\AGOLBackupTools\AGOLBackupTools_helper.py"
   
   The paths and URLs are all dummy values, and do not link to real data.
   
   You first need to set up a credentials text file, with only two lines (username and encoded password). Note encoding 
   avoids storing your password in plain text, but it could be easily decoded! Store the file in a secure location.
   """
   # AGOL credentials file
   cred_file = r"path_to\agol_credentials.txt"
   # Run below once, to create the credentials file.
   # makeCredFile(cred_file, "USERNAME", "PASSWORD")
   
   # Log in and import the toolbox
   loginAGOL(credentials=cred_file)
   arcpy.ImportToolbox(r'path_to\AGOLBackupTools.pyt')

   """
   Example feature services archive procedure.
   """
   backup_folder = r"path_to\backup_folder"
   url_file = r'path_to\urls.txt'  # simple text file with one feature service URL per line. Lines starting with "#" are ignored.
   ArchiveServices(url_file, backup_folder, old_daily=10, old_monthly=12)

   """
   Example 'Update Backup of a Feature Service Layer' procedure. This is used for feature services where new data is 
   frequently added, and the data is not manually edited after it is added (e.g. location tracking data).
   """
   from_data = 'https://locationservices1.arcgis.com/points/FeatureServer/0'
   to_data = r'path_to\backup'
   ServToBkp(from_data, to_data, created_date_field="created_date")

   """
   Example '[Create/Update] Existing Backups of Track Points and Lines'. This will [create/append new] tracking points 
   to a local feature class backup, with an attribute ('use') indicating whether the point has sufficient accuracy for 
   inclusion in track lines. Track lines are then generated for new use=1 points, and appended to the backup track 
   lines layer.
   """
   web_pts = 'https://locationservices1.arcgis.com/points/FeatureServer/0'
   bkp_pts = r'path_to\backup.gdb\bkp_pts'
   if not arcpy.Exists(bkp_pts):
      # Create new feature class backups for points and lines. The lines can then be shared to AGOL as a new feature service.
      bkp_lines = bkp_pts + '_lines'
      arcpy.ArcGISOnlineBackupTools.loc2newbkp(web_pts, bkp_pts, bkp_lines)
      # bkp_lines should then be shared to AGOL as a new feature service
   # Update existing points and lines feature service backups
   bkp_lines_service = 'https://services1.arcgis.com/Tracks_Lines/FeatureServer/0'
   arcpy.ArcGISOnlineBackupTools.loc2bkp(web_pts, bkp_pts, bkp_lines_service)

   return


if __name__ == '__main__':
   main()
