import arcpy

################## This script will reconcile a databases, and for Thermal Monitoring will run calculations then re-reconcile the database for dist ############

# Set variables
TMworkspace = 'Database Connections/Geology_Thermal_Monitoring.sde'
TIworkspace = 'Database Connections/Geology_Thermal_Inventory.sde'
TPworkspace = 'Database Connections/Geology_Temporary_Projects.sde'
Sworkspace = 'Database Connections/Geology_Signage.sde'
arcpy.env.overwriteOutput = True
TMtargetVersion = 'dbo.Thermal_Monitoring'
TItargetVersion = 'dbo.Thermal_Inventory'
TPtargetVersion = 'dbo.Temporary_Projects'
StargetVersion = 'dbo.Signage_Inventory'
fc = "Geology.DBO.Thermal_Monitoring_20181108_LoggerLocations"
fields = ['Logger_Type', 'Log_Formula']
userList = ['wekeller', 'jhungerford', 'bhosseini', 'hwilliams', 'ebwhite']

try:
    ########### Reconcile Thermal Monitoring Database ###########
    # Set the workspace
    arcpy.env.workspace = TMworkspace
    # Create a list of user names that will be used to find versions.
    # find a better way to do this! Move to groups. 

    # Get a list of versions for the service named '???' and '###' to pass into the ReconcileVersions tool.
    versions = arcpy.da.ListVersions(TMworkspace)

    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []

    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == TMtargetVersion.lower():
                    verReconcileList.append(version.name)

    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList) > 0:

        # Execute the ReconcileVersions tool.
        # Put log in a better place in the future
        arcpy.ReconcileVersions_management(TMworkspace, "ALL_VERSIONS", TMtargetVersion, verReconcileList, "LOCK_ACQUIRED",
                                           "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION",
                                           "c:/temp/reconcilelog.txt")

        # Run the compress tool.
        arcpy.Compress_management(TMworkspace)

        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(TMworkspace, "SYSTEM", "", "ALL")

        arcpy.AnalyzeDatasets_management(TMworkspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *********************
        Data Owner(s) Section
        *********************
        '''
        # Get a list of datasets owned by the data owner user (requires second connection file)
        # Get the user name for the workspace
        # this assumes you are using database authentication.
        # OS authentication connection files do not have a 'user' property.
        userName = userList

        # Get a list of all the datasets the user has access to.
        # First, get all the stand alone tables, feature classes and rasters owned by the current user.
        oDataList = arcpy.ListTables('*.' + userName + '.*') + arcpy.ListFeatureClasses('*.' + userName + '.*') + arcpy.ListRasters('*.' + userName + '.*')

        # Next, for feature datasets owned by the current user
        # get all of the featureclasses and add them to the master list.
        for dataset in arcpy.ListDatasets('*.' + userName + '.*'):
           oDataList += arcpy.ListFeatureClasses(feature_dataset=dataset)

        # Rebuild indexes and analyze the data owner tables
        arcpy.RebuildIndexes_management(TMworkspace, "NO_SYSTEM", oDataList, "ALL")

        arcpy.AnalyzeDatasets_management(TMworkspace, "NO_SYSTEM", oDataList, "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *************************
        End Data Owner(s) Section
        *************************
        '''

        # Get a list of all the replica versions in the geodatabase
        replicaVersions = [replica.version for replica in arcpy.da.ListReplicas(TMworkspace)]

        '''
        - We now have a list of versions that were created by taking a map offline (verReconcileList)
        - We also have a list of replica versions (replicaVersions)
        - The versions that we were reconciling are ready to be deleted if they are not currently pointing to a version
        - We are going to loop through the reconcile versions list and remove any versions that are still pointing to a replica
        - The versions remaining in the reconcile list are ready to be cleaned (deleted) up because there are no maps/replicas pointing to them. 
        '''

        # Using the list of versions associated with users/maps that we reconciled earlier. Remove any versions from the list that are still being used by a replica.
        for replicaVer in replicaVersions:
            if replicaVer in verReconcileList:
                verReconcileList.remove(replicaVer)

        # Loop through the versionsList and delete versions that are no longer being referenced by a replica.
        # Since these versions are no longer being referenced by a replica we can assume it's safe to delete them.
        if len(verReconcileList) > 0:     #check to see that the list is not empty
            for version in verReconcileList:
                try:
                    arcpy.DeleteVersion_management(TMworkspace, version)
                except:
                    print("Failed to delete version.")
                    print(arcpy.GetMessages(2))
        else:
            print("No versions to delete.")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")

    ########### Start Edit Session ##############
    # Start an edit session. Must provide the workspace.
    edit = arcpy.da.Editor(TMworkspace)

    ########### Create update cursor to update formula with logger type ############
    # Edit session is started without an undo/redo stack for versioned data

    # Edit session is started without an undo/redo stack for versioned data
    #  (for second argument, use False for unversioned data)
    edit.startEditing(False, True)

    # Start an edit operation
    edit.startOperation()

    with arcpy.da.UpdateCursor(fc, fields) as cursor:
        # For each row, evaluate the Logger_Type value (index position
        # of 0), and update Log_Formula (index position of 1)
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
    with arcpy.da.Editor(TMworkspace) as FieldEdit:
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
    versions = arcpy.da.ListVersions(TMworkspace)
    
    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []
    
    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == TMtargetVersion.lower():
                    verReconcileList.append(version.name)
    
    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList)>0:
        
        # Execute the ReconcileVersions tool.
		# Put log in a better place in the future
        arcpy.ReconcileVersions_management(TMworkspace, "ALL_VERSIONS", TMtargetVersion, verReconcileList, "LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION", "c:/temp/reconcilelog.txt")
        
        # Run the compress tool. 
        arcpy.Compress_management(TMworkspace)
        
        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(TMworkspace, "SYSTEM", "", "ALL")
        
        arcpy.AnalyzeDatasets_management(TMworkspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")
except:
    print(arcpy.GetMessages(2))

print("Thermal Monitoring Reconciled")



    ########### Reconcile Thermal Inventory Database ###########
try:
    # Set the workspace
    arcpy.env.workspace = TIworkspace
    # Create a list of user names that will be used to find versions.
    # find a better way to do this! Move to groups.

    # Get a list of versions for the service named '???' and '###' to pass into the ReconcileVersions tool.
    versions = arcpy.da.ListVersions(TIworkspace)

    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []

    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == TItargetVersion.lower():
                    verReconcileList.append(version.name)

    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList) > 0:

        # Execute the ReconcileVersions tool.
        # Put log in a better place in the future
        arcpy.ReconcileVersions_management(TIworkspace, "ALL_VERSIONS", TItargetVersion, verReconcileList,
                                           "LOCK_ACQUIRED",
                                           "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION",
                                           "c:/temp/reconcilelog.txt")

        # Run the compress tool.
        arcpy.Compress_management(TIworkspace)

        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(TIworkspace, "SYSTEM", "", "ALL")

        arcpy.AnalyzeDatasets_management(TIworkspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA",
                                         "ANALYZE_ARCHIVE")

        '''
        *********************
        Data Owner(s) Section
        *********************
        '''
        # Get a list of datasets owned by the data owner user (requires second connection file)
        # Get the user name for the workspace
        # this assumes you are using database authentication.
        # OS authentication connection files do not have a 'user' property.
        userName = userList

        # Get a list of all the datasets the user has access to.
        # First, get all the stand alone tables, feature classes and rasters owned by the current user.
        oDataList = arcpy.ListTables('*.' + userName + '.*') + arcpy.ListFeatureClasses(
            '*.' + userName + '.*') + arcpy.ListRasters('*.' + userName + '.*')

        # Next, for feature datasets owned by the current user
        # get all of the featureclasses and add them to the master list.
        for dataset in arcpy.ListDatasets('*.' + userName + '.*'):
            oDataList += arcpy.ListFeatureClasses(feature_dataset=dataset)

        # Rebuild indexes and analyze the data owner tables
        arcpy.RebuildIndexes_management(TIworkspace, "NO_SYSTEM", oDataList, "ALL")

        arcpy.AnalyzeDatasets_management(TIworkspace, "NO_SYSTEM", oDataList, "ANALYZE_BASE", "ANALYZE_DELTA",
                                         "ANALYZE_ARCHIVE")

        '''
        *************************
        End Data Owner(s) Section
        *************************
        '''

        # Get a list of all the replica versions in the geodatabase
        replicaVersions = [replica.version for replica in arcpy.da.ListReplicas(TIworkspace)]

        '''
        - We now have a list of versions that were created by taking a map offline (verReconcileList)
        - We also have a list of replica versions (replicaVersions)
        - The versions that we were reconciling are ready to be deleted if they are not currently pointing to a version
        - We are going to loop through the reconcile versions list and remove any versions that are still pointing to a replica
        - The versions remaining in the reconcile list are ready to be cleaned (deleted) up because there are no maps/replicas pointing to them. 
        '''

        # Using the list of versions associated with users/maps that we reconciled earlier. Remove any versions from the list that are still being used by a replica.
        for replicaVer in replicaVersions:
            if replicaVer in verReconcileList:
                verReconcileList.remove(replicaVer)

        # Loop through the versionsList and delete versions that are no longer being referenced by a replica.
        # Since these versions are no longer being referenced by a replica we can assume it's safe to delete them.
        if len(verReconcileList) > 0:  # check to see that the list is not empty
            for version in verReconcileList:
                try:
                    arcpy.DeleteVersion_management(TIworkspace, version)
                except:
                    print("Failed to delete version.")
                    print(arcpy.GetMessages(2))
        else:
            print("No versions to delete.")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")
except:
    print(arcpy.GetMessages(2))

print("Thermal Inventory Reconciled")


########### Reconcile Signage Database ###########
try:
    # Set the workspace
    arcpy.env.workspace = Sworkspace
    # Create a list of user names that will be used to find versions.
    # find a better way to do this! Move to groups.

    # Get a list of versions for the service named '???' and '###' to pass into the ReconcileVersions tool.
    versions = arcpy.da.ListVersions(Sworkspace)

    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []

    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == StargetVersion.lower():
                    verReconcileList.append(version.name)

    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList) > 0:

        # Execute the ReconcileVersions tool.
        # Put log in a better place in the future
        arcpy.ReconcileVersions_management(Sworkspace, "ALL_VERSIONS", StargetVersion, verReconcileList, "LOCK_ACQUIRED",
                                           "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION",
                                           "c:/temp/reconcilelog.txt")

        # Run the compress tool.
        arcpy.Compress_management(Sworkspace)

        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(Sworkspace, "SYSTEM", "", "ALL")

        arcpy.AnalyzeDatasets_management(Sworkspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *********************
        Data Owner(s) Section
        *********************
        '''
        # Get a list of datasets owned by the data owner user (requires second connection file)
        # Get the user name for the workspace
        # this assumes you are using database authentication.
        # OS authentication connection files do not have a 'user' property.
        userName = userList

        # Get a list of all the datasets the user has access to.
        # First, get all the stand alone tables, feature classes and rasters owned by the current user.
        oDataList = arcpy.ListTables('*.' + userName + '.*') + arcpy.ListFeatureClasses('*.' + userName + '.*') + arcpy.ListRasters('*.' + userName + '.*')

        # Next, for feature datasets owned by the current user
        # get all of the featureclasses and add them to the master list.
        for dataset in arcpy.ListDatasets('*.' + userName + '.*'):
           oDataList += arcpy.ListFeatureClasses(feature_dataset=dataset)

        # Rebuild indexes and analyze the data owner tables
        arcpy.RebuildIndexes_management(workspace, "NO_SYSTEM", oDataList, "ALL")

        arcpy.AnalyzeDatasets_management(workspace, "NO_SYSTEM", oDataList, "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

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
        for replicaVer in replicaVersions:
            if replicaVer in verReconcileList:
                verReconcileList.remove(replicaVer)

        # Loop through the versionsList and delete versions that are no longer being referenced by a replica.
        # Since these versions are no longer being referenced by a replica we can assume it's safe to delete them.
        if len(verReconcileList) > 0:     #check to see that the list is not empty
            for version in verReconcileList:
                try:
                    arcpy.DeleteVersion_management(workspace, version)
                except:
                    print("Failed to delete version.")
                    print(arcpy.GetMessages(2))
        else:
            print("No versions to delete.")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")
except:
     print(arcpy.GetMessages(2))

print("Signage Inventory Reconciled")

########### Reconcile Temporary Projects Database ###########
try:
    # Set the workspace
    arcpy.env.workspace = TPworkspace
    # Create a list of user names that will be used to find versions.
    # find a better way to do this! Move to groups.

    # Get a list of versions for the service named '???' and '###' to pass into the ReconcileVersions tool.
    versions = arcpy.da.ListVersions(TPworkspace)

    # Create an empty list that will be used to hold version names that we want to reconcile.
    verReconcileList = []

    # Loop through the list to look for versions with our user names in their name where the parent version is the target version.
    # if these names are found append them to the verReconcileList.
    for user in userList:
        for version in versions:
            if (version.name.lower().find(user.lower()) != -1):
                if version.parentVersionName.lower() == TPtargetVersion.lower():
                    verReconcileList.append(version.name)

    # Perform maintenance if versions are found, otherwise there is no maintenance to perform.
    if len(verReconcileList) > 0:

        # Execute the ReconcileVersions tool.
        # Put log in a better place in the future
        arcpy.ReconcileVersions_management(TPworkspace, "ALL_VERSIONS", TPtargetVersion, verReconcileList, "LOCK_ACQUIRED",
                                           "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION",
                                           "c:/temp/reconcilelog.txt")

        # Run the compress tool.
        arcpy.Compress_management(TPworkspace)

        # Rebuild indexes and analyze the states and states_lineages system tables
        arcpy.RebuildIndexes_management(TPworkspace, "SYSTEM", "", "ALL")

        arcpy.AnalyzeDatasets_management(TPworkspace, "SYSTEM", "", "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *********************
        Data Owner(s) Section
        *********************
        '''
        # Get a list of datasets owned by the data owner user (requires second connection file)
        # Get the user name for the workspace
        # this assumes you are using database authentication.
        # OS authentication connection files do not have a 'user' property.
        userName = userList

        # Get a list of all the datasets the user has access to.
        # First, get all the stand alone tables, feature classes and rasters owned by the current user.
        oDataList = arcpy.ListTables('*.' + userName + '.*') + arcpy.ListFeatureClasses('*.' + userName + '.*') + arcpy.ListRasters('*.' + userName + '.*')

        # Next, for feature datasets owned by the current user
        # get all of the featureclasses and add them to the master list.
        for dataset in arcpy.ListDatasets('*.' + userName + '.*'):
           oDataList += arcpy.ListFeatureClasses(feature_dataset=dataset)

        # Rebuild indexes and analyze the data owner tables
        arcpy.RebuildIndexes_management(TPworkspace, "NO_SYSTEM", oDataList, "ALL")

        arcpy.AnalyzeDatasets_management(TPworkspace, "NO_SYSTEM", oDataList, "ANALYZE_BASE", "ANALYZE_DELTA", "ANALYZE_ARCHIVE")

        '''
        *************************
        End Data Owner(s) Section
        *************************
        '''

        # Get a list of all the replica versions in the geodatabase
        replicaVersions = [replica.version for replica in arcpy.da.ListReplicas(TPworkspace)]

        '''
        - We now have a list of versions that were created by taking a map offline (verReconcileList)
        - We also have a list of replica versions (replicaVersions)
        - The versions that we were reconciling are ready to be deleted if they are not currently pointing to a version
        - We are going to loop through the reconcile versions list and remove any versions that are still pointing to a replica
        - The versions remaining in the reconcile list are ready to be cleaned (deleted) up because there are no maps/replicas pointing to them. 
        '''

        # Using the list of versions associated with users/maps that we reconciled earlier. Remove any versions from the list that are still being used by a replica.
        for replicaVer in replicaVersions:
            if replicaVer in verReconcileList:
                verReconcileList.remove(replicaVer)

        # Loop through the versionsList and delete versions that are no longer being referenced by a replica.
        # Since these versions are no longer being referenced by a replica we can assume it's safe to delete them.
        if len(verReconcileList) > 0:     #check to see that the list is not empty
            for version in verReconcileList:
                try:
                    arcpy.DeleteVersion_management(TPworkspace, version)
                except:
                    print("Failed to delete version.")
                    print(arcpy.GetMessages(2))
        else:
            print("No versions to delete.")

    else:
        print("No versions to reconcile, aborting version maintenance routine.")


except:
    print(arcpy.GetMessages(2))
    
print("Temporary Projects Reconciled")

print("Done")
