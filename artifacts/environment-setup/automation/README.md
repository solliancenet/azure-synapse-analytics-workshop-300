# Environment setup instructions

## Pre-requisites

* Windows PowerShell
* Azure PowerShell

    ```powershell
    if (Get-Module -Name AzureRM -ListAvailable) {
        Write-Warning -Message 'Az module not installed. Having both the AzureRM and Az modules installed at the same time is not supported.'
    } else {
        Install-Module -Name Az -AllowClobber -Scope CurrentUser
    }
    ```

* `Az.CosmosDB` 0.1.4 cmdlet

    ```powershell
    Install-Module -Name Az.CosmosDB -RequiredVersion 0.1.4
    ```

Create the following file: **C:\LabFiles\AzureCreds.ps1**

```powershell
$AzureUserName="odl_user_NNNNNN@msazurelabs.onmicrosoft.com"
$AzurePassword="..."
$TokenGeneratorClientId="1950a258-227b-4e31-a9cf-717495945fc2"
$AzureSQLPassword="..."
```

## Execute setup scripts

* Open PowerShell and change directories to the root of this repo within your local file system.
* Run `Set-ExecutionPolicy Unrestricted`.
* Execute `Connect-AzAccount` and sign in to the ODL user account when prompted.
* Execute `.\artifacts\environment-setup\automation\01-environment-setup.ps1`.
* Execute `.\artifacts\environment-setup\automation\00-cosmosdb-setup.ps1`.
