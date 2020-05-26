Remove-Module solliance-synapse-automation
Import-Module ".\artifacts\environment-setup\solliance-synapse-automation"

$InformationPreference = "Continue"

# These need to be run only if the Az modules are not yet installed
# Install-Module -Name Az -AllowClobber -Scope CurrentUser
# Install-Module -Name Az.CosmosDB -AllowClobber -Scope CurrentUser
# Import-Module Az.CosmosDB

#
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

if ([System.IO.File]::Exists("C:\LabFiles\AzureCreds2.ps1")) {
        
        . C:\LabFiles\AzureCreds2.ps1     
        $userContexts += $AzureUserName2.Split("@")[0].Split("_")[2]
}

Write-Information "Found $($userContexts.Count) user context(s): $([System.String]::Join(", ", $userContexts))"

$overallStateIsValid = $true

foreach ($userContext in $userContexts) {

        Write-Information "Starting to validate user context $($userContext)"

        $asaArtifacts = [ordered]@{

                "wwi02_sale_small_workload_01_asa_$($userContext)" = @{ 
                        Category = "datasets"
                        Valid = $false
                }
                "wwi02_sale_small_workload_02_asa_$($userContext)" = @{ 
                        Category = "datasets"
                        Valid = $false
                }
                "Lab 08 - Execute Business Analyst Queries - $($userContext)" = @{
                        Category = "pipelines"
                        Valid = $false
                }
                "Lab 08 - Execute Data Analyst and CEO Queries - $($userContext)" = @{
                        Category = "pipelines"
                        Valid = $false
                }
                "Lab 06 - Machine Learning - $($userContext)" = @{
                        Category = "notebooks"
                        Valid = $false
                }
                "Lab 07 - Spark ML - $($userContext)" = @{
                        Category = "notebooks"
                        Valid = $false
                }
                "Activity 05 - Model Training - $($userContext)" = @{
                        Category = "notebooks"
                        Valid = $false
                }
                "Lab 05 - Exercise 3 - Column Level Security - $($userContext)" = @{
                        Category = "sqlscripts"
                        Valid = $false
                }
                "Lab 05 - Exercise 3 - Dynamic Data Masking - $($userContext)" = @{
                        Category = "sqlscripts"
                        Valid = $false
                }
                "Lab 05 - Exercise 3 - Row Level Security - $($userContext)" = @{
                        Category = "sqlscripts"
                        Valid = $false
                }
                "Activity 03 - Data Warehouse Optimization - $($userContext)" = @{
                        Category = "sqlscripts"
                        Valid = $false
                }
                "sqlpool01_workload01_$($userContext)" = @{
                        Category = "linkedServices"
                        Valid = $false
                }
                "sqlpool01_workload02_$($userContext)" = @{
                        Category = "linkedServices"
                        Valid = $false
                }
        }

        foreach ($asaArtifactName in $asaArtifacts.Keys) {
                try {
                        Write-Information "Checking $($asaArtifactName) in $($asaArtifacts[$asaArtifactName]["Category"])"
                        $result = Get-ASAObject -WorkspaceName $workspaceName -Category $asaArtifacts[$asaArtifactName]["Category"] -Name $asaArtifactName
                        $asaArtifacts[$asaArtifactName]["Valid"] = $true
                        Write-Information "OK"
                }
                catch {
                        Write-Warning "Not found!"
                        Write-Host $_
                        $overallStateIsValid = $false
                }
        }

        Write-Information "Checking SQLPool $($sqlPoolName)..."
        $sqlPool = Get-SQLPool -SubscriptionId $subscriptionId -ResourceGroupName $resourceGroupName -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName
        if ($sqlPool -eq $null) {
                Write-Warning "    The SQL pool $($sqlPoolName) was not found"
                $overallStateIsValid = $false
        } else {
                Write-Information "OK"

                $tables = [ordered]@{
                        "wwi_perf.Sale_Hash_Ordered_$($userContext)" = @{
                                Count = 339507246
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_perf.Sale_Heap_$($userContext)" = @{
                                Count = 339507246
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_perf.Sale_Index_$($userContext)" = @{
                                Count = 339507246
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_perf.Sale_Partition01_$($userContext)" = @{
                                Count = 339507246
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_perf.Sale_Partition02_$($userContext)" = @{
                                Count = 339507246
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_security.CustomerInfo_$($userContext)" = @{
                                Count = 110
                                StrictCount = $false
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_security.Sale_$($userContext)" = @{
                                Count = 52
                                StrictCount = $false
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_ml.MLModelExt_$($userContext)" = @{
                                Count = 1
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                        "wwi_ml.MLModel_$($userContext)" = @{
                                Count = 0
                                StrictCount = $true
                                Valid = $false
                                ValidCount = $false
                        }
                }
        
$query = @"
SELECT
        S.name as SchemaName
        ,T.name as TableName
FROM
        sys.tables T
        join sys.schemas S on
                T.schema_id = S.schema_id
"@

                #$result = Execute-SQLQuery -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -SQLQuery $query
                $result = Invoke-SqlCmd -Query $query -ServerInstance $sqlEndpoint -Database $sqlPoolName -Username $sqlUser -Password $sqlPassword

                #foreach ($dataRow in $result.data) {
                foreach ($dataRow in $result) {
                        $schemaName = $dataRow[0]
                        $tableName = $dataRow[1]
                
                        $fullName = "$($schemaName).$($tableName)"
                
                        if ($tables[$fullName]) {
                                
                                $tables[$fullName]["Valid"] = $true
                                $strictCount = $tables[$fullName]["StrictCount"]
                
                                Write-Information "Counting table $($fullName) with StrictCount = $($strictCount)..."
                
                                try {
                                $countQuery = "select count_big(*) from $($fullName)"

                                #$countResult = Execute-SQLQuery -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -SQLQuery $countQuery
                                #count = [int64]$countResult[0][0].data[0].Get(0)
                                $countResult = Invoke-Sqlcmd -Query $countQuery -ServerInstance $sqlEndpoint -Database $sqlPoolName -Username $sqlUser -Password $sqlPassword
                                $count = $countResult[0][0]
                
                                Write-Information "    Count result $($count)"
                
                                if (
                                        ($strictCount -and ($count -eq $tables[$fullName]["Count"])) -or
                                        ((-not $strictCount) -and ($count -ge $tables[$fullName]["Count"]))) {

                                        Write-Information "    OK - Records counted is correct."
                                        $tables[$fullName]["ValidCount"] = $true
                                }
                                else {
                                        Write-Warning "    Records counted is NOT correct."
                                        $overallStateIsValid = $false
                                }
                                }
                                catch { 
                                Write-Warning "    Error while querying table."
                                $overallStateIsValid = $false
                                }
                
                        }
                }
                
                # $tables contains the current status of the necessary tables
                foreach ($tableName in $tables.Keys) {
                        if (-not $tables[$tableName]["Valid"]) {
                                Write-Warning "Table $($tableName) was not found."
                                $overallStateIsValid = $false
                        }
                }

                $users = [ordered]@{
                        "CEO_$($userContext)" = @{ Valid = $false }
                        "DataAnalystMiami_$($userContext)" = @{ Valid = $false }
                        "DataAnalystSanDiego_$($userContext)" = @{ Valid = $false }
                        "asa.sql.workload01_$($userContext)" = @{ Valid = $false }
                        "asa.sql.workload02_$($userContext)" = @{ Valid = $false }
                        "odl_user_$($userContext)@msazurelabs.onmicrosoft.com" = @{ Valid = $false }
                }

$query = @"
select name from sys.sysusers
"@
                #$result = Execute-SQLQuery -WorkspaceName $workspaceName -SQLPoolName $sqlPoolName -SQLQuery $query
                $result = Invoke-SqlCmd -Query $query -ServerInstance $sqlEndpoint -Database $sqlPoolName -Username $sqlUser -Password $sqlPassword

                #foreach ($dataRow in $result.data) {
                foreach ($dataRow in $result) {
                        $name = $dataRow[0]

                        if ($users[$name]) {
                                Write-Information "Found user $($name)."
                                $users[$name]["Valid"] = $true
                        }
                }

                foreach ($name in $users.Keys) {
                        if (-not $users[$name]["Valid"]) {
                                Write-Warning "User $($name) was not found."
                                $overallStateIsValid = $false
                        }
                }
        }

        Write-Information "Finished validating user context $($userContext)"
}


if ($overallStateIsValid -eq $true) {
    Write-Information "Validation Passed"
}
else {
    Write-Warning "Validation Failed - see log output"
}


