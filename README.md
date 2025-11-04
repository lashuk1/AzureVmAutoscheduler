# Azure VM Autoscheduler (.NET 8)

Tracks every VM in your Azure subscriptions, logs their current state, and applies basic power rules when needed

## Requirements
  -  .NET 8 SDK
  - `az login` (DefaultAzureCredential)

## Run
```bash
export CSV_PATH="$PWD/vm-log.csv"
export POLL_MINUTES=5
export DRY_RUN=false
export RUNNING_LIMIT_HOURS=8
dotnet restore
dotnet run -c Release
