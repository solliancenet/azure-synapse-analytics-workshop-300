Remove-Module solliance-synapse-automation
Import-Module ".\artifacts\environment-setup\solliance-synapse-automation"

$InformationPreference = "Continue"

# These need to be run only if the Az modules are not yet installed
# Install-Module -Name Az -AllowClobber -Scope CurrentUser
# Install-Module -Name Az.CosmosDB -AllowClobber -Scope CurrentUser
# Import-Module Az.CosmosDB


# Ensure SQLCMD is properly installed:

# Install VC Redist
# https://aka.ms/vs/15/release/vc_redist.x64.exe 

# Install MS ODBC Driver 17 for SQL Server
# https://www.microsoft.com/en-us/download/confirmation.aspx?id=56567 

# Install SQL CMD x64
# https://go.microsoft.com/fwlink/?linkid=2082790

# Install Microsoft Online Services Sign-In Assistant for IT Professionals RTW
# https://www.microsoft.com/en-us/download/details.aspx?id=41950

# Known issue: make sure the ODBC Driver 17 path is BEFORE ODBC Driver 13 in the PATH environment variable

# TODO: Keep all required configuration in C:\LabFiles\AzureCreds.ps1 file
. C:\LabFiles\AzureCreds.ps1

$global:userName = $AzureUserName                # READ FROM FILE
$global:password = $AzurePassword                # READ FROM FILE
$clientId = $TokenGeneratorClientId       # READ FROM FILE
$global:sqlPassword = $AzureSQLPassword          # READ FROM FILE

$securePassword = $password | ConvertTo-SecureString -AsPlainText -Force
$cred = new-object -typename System.Management.Automation.PSCredential -argumentlist $userName, $SecurePassword

Connect-AzAccount -Credential $cred | Out-Null

$resourceGroupName = (Get-AzResourceGroup | Where-Object { $_.ResourceGroupName -like "*L400*" }).ResourceGroupName
$uniqueId =  (Get-AzResourceGroup -Name $resourceGroupName).Tags["DeploymentId"]
$subscriptionId = (Get-AzContext).Subscription.Id
$tenantId = (Get-AzContext).Tenant.Id

$templatesPath = ".\artifacts\environment-setup\templates"
$datasetsPath = ".\artifacts\environment-setup\datasets"
$pipelinesPath = ".\artifacts\environment-setup\pipelines"
$sqlScriptsPath = ".\artifacts\environment-setup\sql"
$workspaceName = "asaworkspace$($uniqueId)"
$cosmosDbAccountName = "asacosmosdb$($uniqueId)"
$cosmosDbDatabase = "CustomerProfile"
$cosmosDbContainer = "OnlineUserProfile01"
$dataLakeAccountName = "asadatalake$($uniqueId)"
$blobStorageAccountName = "asastore$($uniqueId)"
$keyVaultName = "asakeyvault$($uniqueId)"
$keyVaultSQLUserSecretName = "SQL-USER-ASA"
$sqlPoolName = "SQLPool01"
$integrationRuntimeName = "AzureIntegrationRuntime01"
$sparkPoolName = "SparkPool01"
$amlWorkspaceName = "amlworkspace$($uniqueId)"
$global:sqlEndpoint = "$($workspaceName).sql.azuresynapse.net"
$global:sqlUser = "asa.sql.admin"


$ropcBodyCore = "client_id=$($clientId)&username=$($userName)&password=$($password)&grant_type=password"
$global:ropcBodySynapse = "$($ropcBodyCore)&scope=https://dev.azuresynapse.net/.default"
$global:ropcBodyManagement = "$($ropcBodyCore)&scope=https://management.azure.com/.default"
$global:ropcBodySynapseSQL = "$($ropcBodyCore)&scope=https://sql.azuresynapse.net/.default"

$global:synapseToken = ""
$global:synapseSQLToken = ""
$global:managementToken = ""

$global:tokenTimes = [ordered]@{
        Synapse = (Get-Date -Year 1)
        SynapseSQL = (Get-Date -Year 1)
        Management = (Get-Date -Year 1)
}

$userContexts = @( $userName.Split("@")[0].Split("_")[2] )
$userNames = @( $userName )

if ([System.IO.File]::Exists("C:\LabFiles\AzureCreds2.ps1")) {
        
        . C:\LabFiles\AzureCreds2.ps1     
        $userContexts += $AzureUserName2.Split("@")[0].Split("_")[2]
        $userNames += $AzureUserName2
}

$count = 0

Write-Information "Found $($userContexts.Count) user context(s): $([System.String]::Join(", ", $userContexts))"

foreach ($userContext in $userContexts) {

        Write-Information "Starting to process user context $($userContext)"

        Write-Information "Start the $($sqlPoolName) SQL pool if needed."

        $result = Get-SQLPool -SubscriptionId $subscriptionId -ResourceGroupName $resourceGroupName -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName
        if ($result.properties.status -ne "Online") {
        Control-SQLPool -SubscriptionId $subscriptionId -ResourceGroupName $resourceGroupName -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -Action resume
        Wait-ForSQLPool -SubscriptionId $subscriptionId -ResourceGroupName $resourceGroupName -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -TargetStatus Online
        }

        Write-Information "Create SQL logins in master SQL pool"

        $params = @{ 
                PASSWORD = $sqlPassword
                USER_CONTEXT = $userContext
        }
        $result = Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName "master" -FileName "01-create-logins-user" -Parameters $params
        $result

        Write-Information "Create SQL users and role assignments in $($sqlPoolName) $userName"

        $params = @{ 
                USER_NAME = $userNames[$count]
                USER_CONTEXT = $userContext
        }
        $result = Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -FileName "02-create-users-user" -Parameters $params
        $result

        Write-Information "Create tables in the [wwi_ml] schema in $($sqlPoolName)"

        $dataLakeAccountKey = List-StorageAccountKeys -SubscriptionId $subscriptionId -ResourceGroupName $resourceGroupName -Name $dataLakeAccountName
        $params = @{ 
                DATA_LAKE_ACCOUNT_NAME = $dataLakeAccountName  
                DATA_LAKE_ACCOUNT_KEY = $dataLakeAccountKey
                USER_CONTEXT = $userContext
        }
        $result = Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -FileName "05-create-tables-in-wwi-ml-schema-user" -Parameters $params
        $result

        Write-Information "Create tables in the [wwi_security] schema in $($sqlPoolName)"

        $params = @{ 
                DATA_LAKE_ACCOUNT_NAME = $dataLakeAccountName
                USER_CONTEXT = $userContext  
        }
        $result = Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -FileName "06-create-tables-in-wwi-security-schema-user" -Parameters $params
        $result

        Write-Information "Create tables in wwi_perf schema in SQL pool $($sqlPoolName)"

        $params = @{
                USER_CONTEXT = $userContext
        }
        $scripts = [ordered]@{
                "07-create-wwi-perf-sale-heap-user" = "CTAS : Sale_Heap_$($userContext)"
                "08-create-wwi-perf-sale-partition01-user" = "CTAS : Sale_Partition01_$($userContext)"
                "09-create-wwi-perf-sale-partition02-user" = "CTAS : Sale_Partition02_$($userContext)"
                "10-create-wwi-perf-sale-index-user" = "CTAS : Sale_Index_$($userContext)"
                "11-create-wwi-perf-sale-hash-ordered-user" = "CTAS : Sale_Hash_Ordered_$($userContext)"
        }

        foreach ($script in $scripts.Keys) {

                $refTime = (Get-Date).ToUniversalTime()
                Write-Information "Starting $($script) with label $($scripts[$script])"
                
                # initiate the script and wait until it finishes
                Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -FileName $script -Parameters $params
                #Wait-ForSQLQuery -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -Label $scripts[$script] -ReferenceTime $refTime
        }

        Write-Information "Create linked service for SQL pool $($sqlPoolName) with user asa.sql.workload01_$($userContext)"

        $linkedServiceName = "$($sqlPoolName.ToLower())_workload01_$($userContext)"
        $result = Create-SQLPoolKeyVaultLinkedService -TemplatesPath $templatesPath -WorkspaceName $workspaceName -Name $linkedServiceName -DatabaseName $sqlPoolName `
                        -UserName "asa.sql.workload01_$($userContext)" -KeyVaultLinkedServiceName $keyVaultName -SecretName $keyVaultSQLUserSecretName
        Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId

        Write-Information "Create linked service for SQL pool $($sqlPoolName) with user asa.sql.workload02_$($userContext)"

        $linkedServiceName = "$($sqlPoolName.ToLower())_workload02_$($userContext)"
        $result = Create-SQLPoolKeyVaultLinkedService -TemplatesPath $templatesPath -WorkspaceName $workspaceName -Name $linkedServiceName -DatabaseName $sqlPoolName `
                        -UserName "asa.sql.workload02_$($userContext)" -KeyVaultLinkedServiceName $keyVaultName -SecretName $keyVaultSQLUserSecretName
        Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId


        Write-Information "Create data sets for Lab 08"

        $datasets = @{
                "wwi02_sale_small_workload_01_asa" = "$($sqlPoolName.ToLower())_workload01_$($userContext)"
                "wwi02_sale_small_workload_02_asa" = "$($sqlPoolName.ToLower())_workload02_$($userContext)"
        }

        foreach ($dataset in $datasets.Keys) {
                $datasetName = "$($dataset)_$($userContext)"
                Write-Information "Creating dataset $($datasetName)"
                $result = Create-Dataset -DatasetsPath $datasetsPath -WorkspaceName $workspaceName -Name $datasetName -LinkedServiceName $datasets[$dataset] -Parameters $null -FileName $dataset
                Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId
        }

        Write-Information "Create pipelines for Lab 08"

        $params = @{
                USER_CONTEXT = $userContext
        }
        $workloadPipelines = [ordered]@{
                execute_business_analyst_queries_user = "Lab 08 - Execute Business Analyst Queries - $($userContext)"
                execute_data_analyst_and_ceo_queries_user = "Lab 08 - Execute Data Analyst and CEO Queries - $($userContext)"
        }

        foreach ($pipeline in $workloadPipelines.Keys) {
                Write-Information "Creating workload pipeline $($workloadPipelines[$pipeline])"
                $result = Create-Pipeline -PipelinesPath $pipelinesPath -WorkspaceName $workspaceName -Name $workloadPipelines[$pipeline] -FileName $pipeline -Parameters $params
                Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId
        }


        Write-Information "Creating Spark notebooks..."

        $notebooks = [ordered]@{
                "Activity 05 - Model Training" = ".\artifacts\day-03"
                "Lab 06 - Machine Learning" = ".\artifacts\day-03\lab-06-machine-learning"
                "Lab 07 - Spark ML" = ".\artifacts\day-03\lab-07-spark-ml"
        }

        $cellParams = [ordered]@{
                "#SQL_POOL_NAME#" = $sqlPoolName
                "#SUBSCRIPTION_ID#" = $subscriptionId
                "#RESOURCE_GROUP_NAME#" = $resourceGroupName
                "#AML_WORKSPACE_NAME#" = $amlWorkspaceName
                "#USER_CONTEXT#" = $userContext
        }

        foreach ($notebookName in $notebooks.Keys) {

                $notebookFileName = "$($notebooks[$notebookName])\$($notebookName).ipynb"
                $notebookName = "$($notebookName) - $($userContext)"
                Write-Information "Creating notebook $($notebookName) from $($notebookFileName)"
                
                $result = Create-SparkNotebook -TemplatesPath $templatesPath -SubscriptionId $subscriptionId -ResourceGroupName $resourceGroupName `
                        -WorkspaceName $workspaceName -SparkPoolName $sparkPoolName -Name $notebookName -NotebookFileName $notebookFileName -CellParams $cellParams
                $result = Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId
                $result
        }

        Write-Information "Create SQL scripts for Lab 05"

        $sqlScripts = [ordered]@{
                "Lab 05 - Exercise 3 - Column Level Security" = ".\artifacts\day-02\lab-05-security"
                "Lab 05 - Exercise 3 - Dynamic Data Masking" = ".\artifacts\day-02\lab-05-security"
                "Lab 05 - Exercise 3 - Row Level Security" = ".\artifacts\day-02\lab-05-security"
                "Activity 03 - Data Warehouse Optimization" = ".\artifacts\day-02"
        }

        $scriptParams = [ordered]@{
                "#USER_CONTEXT#" = $userContext
        }

        foreach ($sqlScriptName in $sqlScripts.Keys) {
                
                $sqlScriptFileName = "$($sqlScripts[$sqlScriptName])\$($sqlScriptName).sql"
                $sqlScriptName = "$($sqlScriptName) - $($userContext)"
                Write-Information "Creating SQL script $($sqlScriptName) from $($sqlScriptFileName)"
                
                $result = Create-SQLScript -TemplatesPath $templatesPath -WorkspaceName $workspaceName -Name $sqlScriptName -ScriptFileName $sqlScriptFileName -ScriptParams $scriptParams
                $result = Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId
                $result
        }

        Write-Information "Create wwi_poc schema in $($sqlPoolName)"

        $params = @{
                USER_CONTEXT = $userContext
        }
        $result = Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -FileName "16-create-poc-schema-user" -Parameters $params
        $result


        Write-Information "Create tables in wwi_poc schema in SQL pool $($sqlPoolName)"

        $params = @{
                USER_CONTEXT = $userContext
        }
        $scripts = [ordered]@{
                "17-create-wwi-poc-sale-heap-user" = "CTAS : wwi_poc.Sale_$($userContext)"
        }

        foreach ($script in $scripts.Keys) {

                $refTime = (Get-Date).ToUniversalTime()
                Write-Information "Starting $($script) with label $($scripts[$script])"
                
                # initiate the script and wait until it finishes
                Execute-SQLScriptFile -SQLScriptsPath $sqlScriptsPath -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -FileName $script -Parameters $params
                #Wait-ForSQLQuery -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -Label $scripts[$script] -ReferenceTime $refTime
        }

        Write-Information "Create data sets for PoC data load in SQL pool $($sqlPoolName)"

        $params = @{
                USER_CONTEXT = $userContext
        }
        $loadingDatasets = @{
                wwi02_poc_customer_adls = $dataLakeAccountName
                wwi02_poc_customer_asa = $sqlPoolName.ToLower()
        }

        foreach ($dataset in $loadingDatasets.Keys) {
                Write-Information "Creating dataset $($dataset)"
                $result = Create-Dataset -DatasetsPath $datasetsPath -WorkspaceName $workspaceName -Name $dataset -LinkedServiceName $loadingDatasets[$dataset] -Parameters $params
                Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId
        }

        Write-Information "Create pipeline to load PoC data into the SQL pool"

        $params = @{
                BLOB_STORAGE_LINKED_SERVICE_NAME = $blobStorageAccountName
                USER_CONTEXT = $userContext
        }
        $loadingPipelineName = "Setup - Load SQL Pool"
        $fileName = "import_poc_customer_data_user"

        Write-Information "Creating pipeline $($loadingPipelineName)"

        $result = Create-Pipeline -PipelinesPath $pipelinesPath -WorkspaceName $workspaceName -Name $loadingPipelineName -FileName $fileName -Parameters $params
        Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId

        Write-Information "Running pipeline $($loadingPipelineName)"

        $result = Run-Pipeline -WorkspaceName $workspaceName -Name $loadingPipelineName
        $result = Wait-ForPipelineRun -WorkspaceName $workspaceName -RunId $result.runId
        $result

        Write-Information "Deleting pipeline $($loadingPipelineName)"

        $result = Delete-ASAObject -WorkspaceName $workspaceName -Category "pipelines" -Name $loadingPipelineName
        Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId

        foreach ($dataset in $loadingDatasets.Keys) {
                Write-Information "Deleting dataset $($dataset)"
                $result = Delete-ASAObject -WorkspaceName $workspaceName -Category "datasets" -Name $dataset
                Wait-ForOperation -WorkspaceName $workspaceName -OperationId $result.operationId
        }
        
        $count += 1
        
        Write-Information "Finished processing user context $($userContext)"
}
