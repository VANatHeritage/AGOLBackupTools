"""
AGOLBackupTools python toolbox
Author: David Bucklin
Created on: 2021-01-26
Version: ArcGIS Pro / Python 3.x
Toolbox version: v0.2.4
Version date: 2023-11-15

This python toolbox contains tools for copying and archiving feature services from ArcGIS online.

Usage: Import toolbox into ArcGIS Pro (Catalog -> Toolboxes (right click) -> Add Toolbox).
"""
from AGOLBackupTools_helper import *


class Toolbox(object):
   def __init__(self):
      self.label = "ArcGIS Online Backup Tools"
      self.alias = "ArcGISOnlineBackupTools"
      self.description = "Python toolbox with tools for maintaining regular archives of ArcGIS Online feature service " \
                        "layers, and backups for location tracking feature service layers."
      # List of tool classes associated with this toolbox
      self.tools = [fs2fc, archive, fs2bkp, loc2newbkp, loc2bkp]


class fs2fc(object):
   def __init__(self):
      self.label = "Feature Service Layers to Feature Classes"
      self.description = "Download one or more feature layers (and/or tables) from a feature service. This function " \
                         "will attempt to download all records for large feature layers/tables (no limitation " \
                         "on number of rows)."
      self.category = "General Backup Tools"

   def getParameterInfo(self):
      url = arcpy.Parameter(
         displayName="Feature Service URL",
         name="url",
         datatype="GPString",
         parameterType="Required",
         direction="Input")
      lyrs = arcpy.Parameter(
         displayName="Layers to Download (populates after URL is entered)",
         name="lyrs",
         datatype="GPString",
         parameterType="Required",
         direction="Input",
         multiValue=True)
      gdb = arcpy.Parameter(
         displayName="Output Geodatabase",
         name="gdb",
         datatype="DEWorkspace",
         parameterType="Required",
         direction="Input")
      gdb.filter.list = ["Local Database"]
      pref = arcpy.Parameter(
         displayName="Output feature class/table file name prefix",
         name="pref",
         datatype="GPString",
         parameterType="Optional",
         direction="Input")
      fc_out = arcpy.Parameter(
         displayName="Output feature classes/table",
         name="fc_out",
         datatype=["DEFeatureClass", "DETable"],
         parameterType="Derived",
         direction="Output",
         multiValue=True)

      parameters = [url, lyrs, gdb, pref, fc_out]
      return parameters

   def isLicensed(self):  # optional
      if arcpy.GetPortalInfo()['organization'] != '':
         return True  # tool can be executed
      else:
         raise ValueError("No ArcGIS Online connection, are you logged in?")

   def updateParameters(self, parameters):  # optional
      if parameters[0].altered and not parameters[0].hasBeenValidated:
         # set up list of layers (removes 'L[n]' prefix from now)
         da = arcpy.da.Describe(parameters[0].valueAsText)
         if da['dataType'] != 'FeatureClass':
            d = da['children']
            lyrs = [c['name'][len('L' + c['file']):] for c in d]
            parameters[1].filter.list = lyrs
            parameters[1].value = lyrs
         else:
            parameters[1].value = [da['name']]
            parameters[3].value = 'layer_' + da['file']
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):

      # Set parameters
      url = parameters[0].valueAsText
      lyrs = parameters[1].valueAsText.split(";")
      gdb = parameters[2].valueAsText
      pref = parameters[3].valueAsText

      # Check if single layer or feature service
      da = arcpy.da.Describe(url)
      if da['dataType'] == 'FeatureClass':
         ls_out = GetFeatServAll(url, gdb, pref + '_' + lyrs[0])
         parameters[4].value = ls_out
      else:
         # set up list of layers to get (removes 'L[n]' prefix from name)
         d = arcpy.da.Describe(url)['children']
         all_lyrs = [[c['file'], c['name'][len('L' + c['file']):]] for c in d]
         if pref:
            get_lyrs = [[pref + '_' + a[1], url + '/' + a[0]] for a in all_lyrs if a[1] in lyrs]
         else:
            get_lyrs = [[a[1], url + '/' + a[0]] for a in all_lyrs if a[1] in lyrs]
         # get layers
         ls_out = []
         for g in get_lyrs:
            arcpy.AddMessage("Downloading " + g[0] + "...")
            fc_out = GetFeatServAll(g[1], gdb, g[0])
            ls_out.append(fc_out)
         parameters[4].value = ls_out
      return ls_out


class archive(object):
   def __init__(self):
      self.label = "Archive Feature Services by URL"
      self.description = "Given a list of URLS in a text file (one per line) and an archive folder, archives " \
                         "feature service layers (downloads datestamped copies of all layers). Will use existing " \
                         "geodatabases in the folder if they already exist, or create a new one if it doesn't exist. " \
                         "One-per-day and one-per-month archives are maintained. Daily and monthly archives older " \
                         "than the specified limits are deleted."
      self.category = "General Backup Tools"

   def getParameterInfo(self):
      urls = arcpy.Parameter(
         displayName="Text file with list of feature service URLs",
         name="urls",
         datatype="DEFile",
         parameterType="Required",
         direction="Input")
      urls.filter.list = ['txt']
      fold = arcpy.Parameter(
         displayName="Archive folder",
         name="backup_folder",
         datatype="DEFolder",
         parameterType="Required",
         direction="Input")
      old_day = arcpy.Parameter(
         displayName="Delete daily archive layers older than (days):",
         name="old_day",
         datatype="Long",
         parameterType="Required",
         direction="Input")
      old_day.value = 10
      old_month = arcpy.Parameter(
         displayName="Delete monthly archive layers older than (months):",
         name="old_month",
         datatype="Long",
         parameterType="Required",
         direction="Input")
      old_month.value = 12

      parameters = [urls, fold, old_day, old_month]
      return parameters

   def isLicensed(self):  # optional
      if arcpy.GetPortalInfo()['organization'] != '':
         return True  # tool can be executed
      else:
         raise ValueError("No ArcGIS Online connection, are you logged in?")

   def updateParameters(self, parameters):  # optional
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):
      # Set parameters
      urls = parameters[0].valueAsText
      fold = parameters[1].valueAsText
      old_days = parameters[2].value
      old_months = parameters[3].value
      ArchiveServices(urls, fold, old_days, old_months)
      arcpy.AddMessage("Finished with archiving.")
      return True


class fs2bkp(object):
   def __init__(self):
      self.label = "Update Backup of a Feature Service Layer"
      self.description = "Compares a source feature service layer to an existing backup feature service layer or " \
                         "feature class. Using a date created field to find rows not present in the backup, copies " \
                         "the new rows from the source to the backup."
      self.category = "General Backup Tools"

   def getParameterInfo(self):
      from_data = arcpy.Parameter(
         displayName="Source Feature Service Layer",
         name="from_data",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      to_data = arcpy.Parameter(
         displayName="Backup Feature Class or Service Layer (to update)",
         name="to_data",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      created_date_field = arcpy.Parameter(
         displayName="Created Date field",
         name="created_date_field",
         datatype="GPString",
         parameterType="Required",
         direction="Input")

      parameters = [from_data, to_data, created_date_field]
      return parameters

   def isLicensed(self):  # optional
      if arcpy.GetPortalInfo()['organization'] != '':
         return True  # tool can be executed
      else:
         raise ValueError("No ArcGIS Online connection, are you logged in?")

   def updateParameters(self, parameters):  # optional
      if parameters[0].altered and parameters[0].value and not parameters[2].altered:
         v = parameters[0].value
         nms = [f.name for f in arcpy.ListFields(v) if f.type == 'Date']
         parameters[2].filter.list = nms
         if 'created_date' in nms:
            parameters[2].value = 'created_date'
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):
      # Set parameters
      from_data = parameters[0].valueAsText
      to_data = parameters[1].valueAsText
      created_date_field = parameters[2].value
      # from_path = arcpy.Describe(from_data).catalogPath
      # to_path = arcpy.Describe(to_data).catalogPath
      ServToBkp(from_data, to_data, created_date_field)
      return to_data


class loc2newbkp(object):
   def __init__(self):
      self.label = "Create Backups for Track Points and Lines"
      self.description = "Copies data from an existing track points feature service layer to a new feature class. " \
                         "Points are attributed with a 'use' column, indicating if they should be used to generate " \
                         "track lines, and lines are then generated from these points. These feature classes" \
                         " (or feature services generated from them) can then be used as the backup layer inputs to " \
                         "the 'Update Backups of Track Points and Track Lines' tool."
      self.category = "Location Tracking Backup Tools"

   def getParameterInfo(self):
      web_pts = arcpy.Parameter(
         displayName="Source Track Points Feature Service Layer",
         name="web_pts",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      web_pts.filter.list = ["Point"]
      # Ouptut (to create new)
      loc_pts_new = arcpy.Parameter(
         displayName="Output Track Points Feature Class",
         name="loc_pts_new",
         datatype="DEFeatureClass",
         parameterType="Required",
         direction="Output")
      loc_pts_new.value = 'track_pts'
      lines_new = arcpy.Parameter(
         displayName="Output Track Lines Feature Class",
         name="lines_new",
         datatype="DEFeatureClass",
         parameterType="Required",
         direction="Output")
      lines_new.value = 'track_lines'

      break_by = arcpy.Parameter(
         displayName="Group track points using:",
         name="break_by",
         datatype="GPString",
         parameterType="Required",
         direction="Input")
      break_by.filter.type = "ValueList"
      break_by.filter.list = ["user_date", "session_id"]  # , "full_name"]
      break_by.value = "user_date"

      break_tracks_seconds = arcpy.Parameter(
         displayName="Track lines time gap (seconds)",
         name="break",
         datatype="GPLong",
         parameterType="Required",
         direction="Input")
      break_tracks_seconds.value = 600
      parameters = [web_pts, loc_pts_new, lines_new, break_by, break_tracks_seconds]
      return parameters

   def isLicensed(self):  # optional
      if arcpy.GetPortalInfo()['organization'] != '':
         return True  # tool can be executed
      else:
         raise ValueError("No ArcGIS Online connection, are you logged in?")

   def updateParameters(self, parameters):
      # if parameters[0].altered and not parameters[0].hasBeenValidated:
      #    new_nm = os.path.basename(parameters[0].valueAsText) + '_' + datetime.datetime.now().strftime('%Y%m%d')
      #    if not parameters[1].hasBeenValidated:
      #       parameters[1].value = new_nm
      #    if not parameters[2].hasBeenValidated:
      #       parameters[2].value = new_nm + '_lines'
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):
      web_pts = parameters[0].valueAsText
      loc_pts = parameters[1].valueAsText
      lines = parameters[2].valueAsText
      break_by = parameters[3].valueAsText
      break_tracks_seconds = int(parameters[4].valueAsText)

      arcpy.AddMessage("Creating new backup layers...")
      GetFeatServAll(web_pts, os.path.dirname(loc_pts), os.path.basename(loc_pts))
      add_user_date(loc_pts)  # This adds a 'user_date' attribute to loc_pts.

      # Run track line generation process
      prep_track_pts(loc_pts, break_by, break_tracks_seconds=break_tracks_seconds)
      make_track_lines(loc_pts, lines)
      return loc_pts, lines


class loc2bkp(object):
   def __init__(self):
      self.label = "Update Backups of Track Points and Lines"
      self.description = "Compares a track points feature service layer to an existing track points feature class or " \
                         "feature service layer backup. Using a date created field to find rows not present in the " \
                         "backup, copies the new rows from the source to the backup. It then builds track lines for " \
                         "the new points, and appends the lines to the backup track lines layer " \
                         "(which can be a feature class or feature service layer)."
      self.category = "Location Tracking Backup Tools"

   def getParameterInfo(self):
      web_pts = arcpy.Parameter(
         displayName="Source Track Points Feature Service Layer",
         name="web_pts",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      web_pts.filter.list = ["Point"]
      loc_pts = arcpy.Parameter(
         displayName="Backup Track Points (to update)",
         name="loc_pts",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      loc_pts.filter.list = ["Point"]
      lines = arcpy.Parameter(
         displayName="Backup Track Lines (to update)",
         name="lines",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      lines.filter.list = ["Polyline"]

      break_by = arcpy.Parameter(
         displayName="Group track points using:",
         name="break_by",
         datatype="GPString",
         parameterType="Required",
         direction="Input")
      break_by.filter.type = "ValueList"
      break_by.filter.list = ["user_date", "session_id"]  # , "full_name"]
      break_by.value = "user_date"

      break_tracks_seconds = arcpy.Parameter(
         displayName="Track lines time gap (seconds)",
         name="break",
         datatype="GPLong",
         parameterType="Required",
         direction="Input")
      break_tracks_seconds.value = 600
      parameters = [web_pts, loc_pts, lines, break_by, break_tracks_seconds]
      return parameters

   def isLicensed(self):  # optional
      if arcpy.GetPortalInfo()['organization'] != '':
         return True  # tool can be executed
      else:
         raise ValueError("No ArcGIS Online connection, are you logged in?")

   def updateParameters(self, parameters):  # optional
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):
      web_pts = parameters[0].valueAsText
      loc_pts = parameters[1].valueAsText
      lines = parameters[2].valueAsText
      break_by = parameters[3].valueAsText
      break_tracks_seconds = int(parameters[4].valueAsText)
      arcpy.AddMessage('Looking for new points in ' + web_pts + '.')
      # Temp datasets
      new_pts = arcpy.env.scratchGDB + os.sep + 'tmp_' + os.path.basename(loc_pts)
      tmp_pts = arcpy.env.scratchGDB + os.sep + 'tmp_pts'
      tmp_lines = arcpy.env.scratchGDB + os.sep + 'tmp_lines'
      ServToBkp(web_pts, loc_pts, created_date_field="created_date", append_data=new_pts)

      # If there is new data, update track lines, using ALL points for the specific tracks.
      if arcpy.GetCount_management(new_pts)[0] != '0':
         # Find unique track names with any new points, make a copy of them
         uniq_trk = list(set([a[0] for a in arcpy.da.SearchCursor(new_pts, break_by)]))
         uniq_trk_q = break_by + " IN ('" + "','".join(uniq_trk) + "')"
         arcpy.Select_analysis(loc_pts, tmp_pts, uniq_trk_q)
         print("Making new track lines for " + arcpy.GetCount_management(tmp_pts)[0] + " points.")
         prep_track_pts(tmp_pts, break_by=break_by, break_tracks_seconds=break_tracks_seconds)
         make_track_lines(tmp_pts, tmp_lines)
         # Now that lines are made, delete the original points from main layer, and then append the updated points.
         lyr_pt = arcpy.MakeFeatureLayer_management(loc_pts, where_clause=uniq_trk_q)
         with arcpy.da.UpdateCursor(lyr_pt, [break_by]) as curs:
            for row in curs:
               if row[0] in uniq_trk:
                  curs.deleteRow()
         del lyr_pt
         arcpy.Append_management(tmp_pts, loc_pts, "NO_TEST")
         # exit now if no lines were generated (e.g. if there was only one point per track line)
         if arcpy.GetCount_management(tmp_lines)[0] == '0':
            print("No track lines generated, no updates to be made.")
            return
         # Remove existing lines, by unique track ID
         arcpy.AddMessage("Updating track lines layer...")
         lyr_line = arcpy.MakeFeatureLayer_management(lines,  where_clause=uniq_trk_q)
         with arcpy.da.UpdateCursor(lyr_line, [break_by]) as curs:
            for row in curs:
               if row[0] in uniq_trk:
                  curs.deleteRow()
         # headsup: DeleteRows_management (below) stopped working on feature service all of a sudden! Changed to the updateCursor->deleteRow approach above, which is working.
         #  error: arcgisscripting.ExecuteError: ERROR 160236: The operation is not supported by this implementation.
         # arcpy.SelectLayerByAttribute_management(lyr_line, "NEW_SELECTION", uniq_trk_q)
         # if not arcpy.GetCount_management(lyr_line)[0] == '0':
         #    arcpy.DeleteRows_management(lyr_line)
         del lyr_line
         # Check if spatial references are the same. If not, project new data to match the destination layer.
         sr0 = arcpy.Describe(tmp_lines).spatialReference.name
         sr1 = arcpy.Describe(lines).spatialReference.name
         if sr0 != sr1:
            arcpy.AddMessage("Projecting lines...")
            tmp_lines_proj = arcpy.env.scratchGDB + os.sep + 'tmp_lines_proj'
            arcpy.Project_management(tmp_lines, tmp_lines_proj, lines)
            arcpy.Append_management(tmp_lines_proj, lines, "NO_TEST")
         else:
            arcpy.Append_management(tmp_lines, lines, "NO_TEST")
         ct = arcpy.GetCount_management(tmp_lines)[0]
         arcpy.AddMessage("Appended " + ct + " new track lines.")
      else:
         arcpy.AddMessage("No new data, no updates made.")
      return lines


# end

