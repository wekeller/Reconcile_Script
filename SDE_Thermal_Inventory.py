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
