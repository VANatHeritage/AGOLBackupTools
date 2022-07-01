"""
AGOLBackupTools python toolbox
Author: David Bucklin
Created on: 2021-01-26
Version: ArcGIS Pro / Python 3.x

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
                         "will attempt to download the full dataset for large feature layers/tables (no limitation " \
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
         d = arcpy.da.Describe(parameters[0].valueAsText)['children']
         lyrs = [c['name'][len('L' + c['file']):] for c in d]
         parameters[1].filter.list = lyrs
         parameters[1].value = lyrs
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):

      # Set parameters
      url = parameters[0].valueAsText
      lyrs = parameters[1].valueAsText.split(";")
      gdb = parameters[2].valueAsText
      pref = parameters[3].valueAsText

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
      self.description = "Given a list of URLS in a text file (one per line), and an archive folder, archives " \
                         "feature service layers. Will use existing geodatabases in the folder if they already " \
                         "exist, or create a new one if it doesn't exist. One-per-day and one-per-month archives " \
                         "are saved. Daily and monthly archives older than the specified limits are deleted."
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
      self.description = "Compares a source feature service layer to an existing backup feature service " \
                         "layer or feature class. Using a date created field to find rows not present in the backup," \
                         " copies the new rows from the source to the backup."
      self.category = "General Backup Tools"
   def getParameterInfo(self):
      from_data = arcpy.Parameter(
         displayName="Source Feature Service Layer",
         name="from_data",
         datatype="GPFeatureLayer",
         parameterType="Required",
         direction="Input")
      to_data = arcpy.Parameter(
         displayName="Backup Feature Service Layer or Feature Class (to update)",
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
      self.label = "Create New Backups for Track Points and Lines"
      self.description = "Copies data from an existing track points feature service layer to a new feature class. " \
                         "Points are attributed with a 'use' column, indicating if they should be used to generate " \
                         "track lines. Lines are then generated from these points. These layers (or feature " \
                         "services generated from them) can then be used as the backup layer inputs to the " \
                         "'Update Backups of Track Points and Track Lines' tool."
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
         displayName="Backup Track Points Feature Class (to create)",
         name="loc_pts_new",
         datatype="DEFeatureClass",
         parameterType="Required",
         direction="Output")
      lines_new = arcpy.Parameter(
         displayName="Backup Track Lines (to create)",
         name="lines_new",
         datatype="DEFeatureClass",
         parameterType="Required",
         direction="Output")
      break_tracks_seconds = arcpy.Parameter(
         displayName="Track lines time gap (seconds)",
         name="break",
         datatype="GPLong",
         parameterType="Required",
         direction="Input")
      break_tracks_seconds.value = 600
      # coulddo: add argument for break_tracks_seconds value?
      parameters = [web_pts, loc_pts_new, lines_new, break_tracks_seconds]
      return parameters

   def isLicensed(self):  # optional
      if arcpy.GetPortalInfo()['organization'] != '':
         return True  # tool can be executed
      else:
         raise ValueError("No ArcGIS Online connection, are you logged in?")

   def updateParameters(self, parameters):
      return

   def updateMessages(self, parameters):  # optional
      return

   def execute(self, parameters, messages):
      arcpy.env.workspace = arcpy.env.scratchGDB
      web_pts = parameters[0].valueAsText
      loc_pts = parameters[1].valueAsText
      lines = parameters[2].valueAsText
      break_tracks_seconds = int(parameters[3].valueAsText)
      arcpy.AddMessage("Creating new backup layers...")
      GetFeatServAll(web_pts, os.path.dirname(loc_pts), os.path.basename(loc_pts))
      prep_track_pts(loc_pts, by_session=True, break_tracks_seconds=break_tracks_seconds)
      make_track_lines(loc_pts, lines)
      return loc_pts, lines


class loc2bkp(object):
   def __init__(self):
      self.label = "Update Existing Backups of Track Points and Lines"
      self.description = "Compares a track points feature service layer to an existing track points feature class or " \
                         "feature service layer backup. Using a date created field to find rows not present in the " \
                         "backup copies the new rows from the source to the backup. It then builds track lines for " \
                         "new the updated points, and appends the lines to the backup track lines layer " \
                         "(feature class or feature service layer)."
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
         displayName="Backup Track Points Feature Class (to update)",
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
      break_tracks_seconds = arcpy.Parameter(
         displayName="Track lines time gap (seconds)",
         name="break",
         datatype="GPLong",
         parameterType="Required",
         direction="Input")
      break_tracks_seconds.value = 600
      parameters = [web_pts, loc_pts, lines, break_tracks_seconds]
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
      arcpy.env.workspace = arcpy.env.scratchGDB
      web_pts = parameters[0].valueAsText
      loc_pts = parameters[1].valueAsText
      lines = parameters[2].valueAsText
      break_tracks_seconds = int(parameters[3].valueAsText)
      arcpy.AddMessage('Looking for new points in ' + web_pts + '.')
      new_pts = 'tmp_' + os.path.basename(loc_pts)
      ServToBkp(web_pts, loc_pts, created_date_field="created_date", append_data=new_pts)
      # If there is new data, update track lines.
      if arcpy.GetCount_management(new_pts)[0] != '0':
         # Take ALL track points from sessions with any new points, copy them
         sess = list(set([a[0] for a in arcpy.da.SearchCursor(new_pts, 'session_id')]))
         lyr_pt = arcpy.MakeFeatureLayer_management(loc_pts)
         arcpy.SelectLayerByAttribute_management(lyr_pt, 'NEW_SELECTION', "session_id IN ('" + "','".join(sess) + "')")
         arcpy.CopyFeatures_management(lyr_pt, 'tmp_pts')
         # delete those points from the local layer, since they need to be updated
         arcpy.DeleteRows_management(lyr_pt)
         del lyr_pt
         print("Making new track lines for " + arcpy.GetCount_management('tmp_pts')[0] + " points.")
         prep_track_pts('tmp_pts', by_session=True, break_tracks_seconds=break_tracks_seconds)
         make_track_lines('tmp_pts', 'tmp_lines')
         # Now that lines are made, append points back to local points layer
         arcpy.Append_management('tmp_pts', loc_pts, "NO_TEST")
         # exit now if no lines were generated (e.g. only one point for track line)
         if arcpy.GetCount_management('tmp_lines')[0] == '0':
            print("No track lines generated, no updates to be made.")
            return
         # Remove existing lines, by session
         arcpy.AddMessage("Updating track lines layer...")
         lyr = arcpy.MakeFeatureLayer_management(lines)
         arcpy.SelectLayerByAttribute_management(lyr, "NEW_SELECTION", "session_id IN ('" + "','".join(sess) + "')")
         arcpy.DeleteRows_management(lyr)
         del lyr
         # Check if spatial references are the same. If not, project new data to match the destination layer.
         sr0 = arcpy.Describe('tmp_lines').spatialReference.name
         sr1 = arcpy.Describe(lines).spatialReference.name
         if sr0 != sr1:
            arcpy.AddMessage("Projecting lines...")
            arcpy.Project_management('tmp_lines', 'tmp_lines_proj', lines)
            arcpy.Append_management('tmp_lines_proj', lines, "NO_TEST")
         else:
            arcpy.Append_management('tmp_lines', lines, "NO_TEST")
         ct = arcpy.GetCount_management(lines)[0]
         arcpy.AddMessage("Appended " + ct + " new track lines.")
      else:
         arcpy.AddMessage("No new data, no updates made.")
      return lines


# end

