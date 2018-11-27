import arcpy

################## This script will reconcile a database, run calculations then re-reconcile the database for dist ############
try:
    # Set the workspace
    workspace = 'Database Connections/Thermal_Monitoring_Admin.sde'
    arcpy.env.workspace = workspace

    # Set variables
    arcpy.env.overwriteOutput = True
    targetVersion = 'dbo.Thermal_Monitoring'
    fc = "Geology.DBO.Thermal_Monitoring_20181108_LoggerLocations"
    fields = ['Logger_Type', 'Log_Formula']

    ########### Reconcile Databases ###########

    # Create a list of user names that will be used to find versions.
    # find a better way to do this!
    userList = ['WEKeller', 'JHungerford', 'BHosseini', 'HWilliams', 'EBWhite']

    # Get a list of versions for the service named '???' and '###' to pass into the ReconcileVersions tool.
    versions = arcpy.da.ListVersions(workspace)

    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []

    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == targetVersion.lower():
                    verReconcileList.append(version.name)

    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList) > 0:

        # Execute the ReconcileVersions tool.
        # Put log in a better place in the future
        arcpy.ReconcileVersions_management(workspace, "ALL_VERSIONS", targetVersion, verReconcileList, "LOCK_ACQUIRED",
                                           "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION",
                                           "c:/temp/reconcilelog.txt")

        # Run the compress tool.
        arcpy.Compress_management(workspace)

        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(workspace, "SYSTEM", "", "ALL")

        arcpy.AnalyzeDatasets_management(workspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *********************
        Data Owner(s) Section
        *********************
        '''
        # Get a list of datasets owned by the data owner user (requires second connection file)

        # Set the workspace
        # arcpy.env.workspace = 'Database Connections/dataOwner.sde'

        # Set a variable for the workspace
        # workspace = arcpy.env.workspace

        # Get the user name for the workspace
        # this assumes you are using database authentication.
        # OS authentication connection files do not have a 'user' property.
        # userName = arcpy.Describe(arcpy.env.workspace).connectionProperties.user

        # Get a list of all the datasets the user has access to.
        # First, get all the stand alone tables, feature classes and rasters owned by the current user.
        # oDataList = arcpy.ListTables('*.' + userName + '.*') + arcpy.ListFeatureClasses('*.' + userName + '.*') + arcpy.ListRasters('*.' + userName + '.*')

        # Next, for feature datasets owned by the current user
        # get all of the featureclasses and add them to the master list.
        # for dataset in arcpy.ListDatasets('*.' + userName + '.*'):
        #    oDataList += arcpy.ListFeatureClasses(feature_dataset=dataset)

        # Rebuild indexes and analyze the data owner tables
        # arcpy.RebuildIndexes_management(workspace, "NO_SYSTEM", oDataList, "ALL")

        # arcpy.AnalyzeDatasets_management(workspace, "NO_SYSTEM", oDataList, "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *************************
        End Data Owner(s) Section
        *************************
        '''

        # Get a list of all the replica versions in the geodatabase
        replicaVersions = [replica.version for replica in arcpy.da.ListReplicas(workspace)]

        '''
        - We now have a list of versions that were created by taking a map offline (verReconcileList)
        - We also have a list of replica versions (replicaVersions)
        - The versions that we were reconciling are ready to be deleted if they are not currently pointing to a version
        - We are going to loop through the reconcile versions list and remove any versions that are still pointing to a replica
        - The versions remaining in the reconcile list are ready to be cleaned (deleted) up because there are no maps/replicas pointing to them. 
        '''

        # Using the list of versions associated with users/maps that we reconciled earlier. Remove any versions from the list that are still being used by a replica.
        # for replicaVer in replicaVersions:
        # if replicaVer in verReconcileList:
        # verReconcileList.remove(replicaVer)

        # Loop through the versionsList and delete versions that are no longer being referenced by a replica.
        # Since these versions are no longer being referenced by a replica we can assume it's safe to delete them.
        # if len(verReconcileList) > 0:     #check to see that the list is not empty
        # for version in verReconcileList:
        # try:
        # arcpy.DeleteVersion_management(workspace, version)
        # except:
        # print("Failed to delete version.")
        # print(arcpy.GetMessages(2))
        # else:
        # print("No versions to delete.")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")

    ########### Start Edit Session ##############
    # Start an edit session. Must provide the workspace.
    edit = arcpy.da.Editor(workspace)

    ########### Create update cursor to update formula with logger type ############
    # Edit session is started without an undo/redo stack for versioned data

    # Edit session is started without an undo/redo stack for versioned data
    #  (for second argument, use False for unversioned data)
    edit.startEditing(False, True)

    # Start an edit operation
    edit.startOperation()

    with arcpy.da.UpdateCursor(fc, fields) as cursor:
        # For each row, evaluate the Logger_Type value (index position
        # of 0), and update Log_Formul (index position of 1)
        for row in cursor:
            if (row[0] == 'U23'):
                row[1] = '0.5017'
            elif (row[0] == 'U12'):
                row[1] = '0.5017'
            elif (row[0] == 'MX2304'):
                row[1] = '0.9799'
            elif (row[0] == 'Micro H21'):
                row[1] = '4.0421'
            elif (row[0] == 'Micro'):
                row[1] = '4.0421'
            else:
                row[1] = '1000'
            # Update the cursor with the updated list
            cursor.updateRow(row)

    # Stop the edit operation.
    edit.stopOperation()

    # Stop the edit session and save the changes
    edit.stopEditing(True)

    ########### Field Calculate Days Until Due - Thermal Monitoring ###########
    # calculations
    with arcpy.da.Editor(workspace) as FieldEdit:
        # multiply Log_Formula field by Interval field: outputs number of days until data full
        arcpy.CalculateField_management((fc), 'Log_F_Calc', '[Log_Formula]* [Interval]', 'VB', '#')
        # add number of days until data full to date downloaded to get date due field
        arcpy.CalculateField_management((fc), 'Date_Due', 'DateAdd ("d", [Log_F_Calc], [Date_Downloaded] )', 'VB', '#')
        # subtracts date due from todays date to get number of days until full
        arcpy.CalculateField_management((fc), 'Days_Due', 'DateDiff ("d", NOW(), [Date_Due] )', 'VB', '#')
        # subtracts date installed from today's date to give total number of days with data
        arcpy.CalculateField_management((fc), 'Days_Data', 'DateDiff ("d", [Date_Installed], [Date_Downloaded] )', 'VB', '#')




        ########### Reconcile Databases ###########
    
    # Create a list of user names that will be used to find versions.
	# find a better way to do this!
    
    # Get a list of versions for the service named '???' and '###' to pass into the ReconcileVersions tool.
    versions = arcpy.da.ListVersions(workspace)
    
    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []
    
    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == targetVersion.lower():
                    verReconcileList.append(version.name)
    
    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList)>0:
        
        # Execute the ReconcileVersions tool.
		# Put log in a better place in the future
        arcpy.ReconcileVersions_management(workspace, "ALL_VERSIONS", targetVersion, verReconcileList, "LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION", "c:/temp/reconcilelog.txt")
        
        # Run the compress tool. 
        arcpy.Compress_management(workspace)
        
        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(workspace, "SYSTEM", "", "ALL")
        
        arcpy.AnalyzeDatasets_management(workspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")

except:
    print(arcpy.GetMessages(2))
    
print("Done.")
