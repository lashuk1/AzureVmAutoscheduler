using System.Collections.Concurrent;
using Azure;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Compute;
using Azure.ResourceManager.Compute.Models;
using Azure.ResourceManager.Resources;
using System.Globalization; 


string CsvPath = Environment.GetEnvironmentVariable("CSV_PATH") ?? "vm-log.csv";
int PollMinutes = int.TryParse(Environment.GetEnvironmentVariable("POLL_MINUTES"), out var pm) ? pm : 5;
bool DryRun = string.Equals(Environment.GetEnvironmentVariable("DRY_RUN"), "true", StringComparison.OrdinalIgnoreCase);
double RunningLimitHours = double.TryParse(Environment.GetEnvironmentVariable("RUNNING_LIMIT_HOURS"), 
    NumberStyles.Float, CultureInfo.InvariantCulture, 
    out var h
) ? h : 8.0;

Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Azure VM Autoscheduler started");
Console.WriteLine($"CSV_PATH={CsvPath}, POLL_MINUTES={PollMinutes}, DRY_RUN={DryRun}, RUNNING_LIMIT_HOURS={RunningLimitHours}");

var seenRunning = new ConcurrentDictionary<string, DateTime>();
await EnsureHeaderAsync(CsvPath);

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

while (!cts.IsCancellationRequested)
{
    try
    {
        await PollAsync(CsvPath, seenRunning, RunningLimitHours, DryRun);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] ERROR: {ex.Message}");
    }

    try
    {
        await Task.Delay(TimeSpan.FromMinutes(PollMinutes), cts.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }
}

static async Task PollAsync(
    string csvPath,
    ConcurrentDictionary<string, DateTime> seen,
    double limitHrs,
    bool dryRun)
{
    var arm = new ArmClient(new DefaultAzureCredential());

    foreach (var sub in arm.GetSubscriptions().GetAll())
    {
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Checking subscription {sub.Data.DisplayName}");

        foreach (var vm in sub.GetVirtualMachines())
        {
            try
            {
                var vmData = vm.Data;
                string vmId = vm.Id;
                string rg = vm.Id.ResourceGroupName;
                string name = vmData.Name;
                string subId = vm.Id.SubscriptionId;
                string powerDisplay = "";

                bool hasAuto = vmData.Tags.TryGetValue("Autoshutdown", out var tagVal) && tagVal == "1";

                if (hasAuto)
                {
                    
                    var vmWithIv = await vm.GetAsync(expand: InstanceViewType.InstanceView);
                    var iv = vmWithIv.Value.Data.InstanceView;
                    powerDisplay = iv?.Statuses?
                        .FirstOrDefault(s => s.Code != null && s.Code.StartsWith("PowerState/", StringComparison.OrdinalIgnoreCase))
                        ?.DisplayStatus ?? "Unknown";


                    if (IsRunning(powerDisplay))
                    {
                        var now = DateTime.UtcNow;
                        var first = seen.GetOrAdd(vmId, now);
                        var runningHrs = (now - first).TotalHours;

                        if (runningHrs > limitHrs)
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] {name} running {runningHrs:F2}h > {limitHrs}h → PowerOff {(dryRun ? "(dry)" : "")}");
                            if (!dryRun)
                                await vm.PowerOffAsync(WaitUntil.Completed, false);
                            seen.TryRemove(vmId, out _);
                        }
                    }
                    else
                    {
                        seen.TryRemove(vmId, out _);
                        if (IsStoppedAllocated(powerDisplay))
                        {
                            Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] {name} stopped (allocated) → Deallocate {(dryRun ? "(dry)" : "")}");
                            if (!dryRun)
                                await vm.DeallocateAsync(WaitUntil.Completed);
                        }
                    }
                }

                await AppendCsvAsync(csvPath, DateTime.UtcNow, subId, rg, name, powerDisplay);
            }
            catch (Exception exVm)
            {
                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] VM ERROR: {exVm.Message}");
            }
        }
    }
}

static bool IsRunning(string display) =>
    display.Contains("running", StringComparison.OrdinalIgnoreCase);

static bool IsStoppedAllocated(string display) =>
    display.Contains("stopped", StringComparison.OrdinalIgnoreCase)
    && !display.Contains("deallocated", StringComparison.OrdinalIgnoreCase);

static async Task EnsureHeaderAsync(string path)
{
    if (!File.Exists(path))
        await File.WriteAllTextAsync(path, "TimestampUtc,SubscriptionId,ResourceGroup,ComputerName,PowerState\n");
}

static Task AppendCsvAsync(string path, DateTime tsUtc, string subId, string rg, string name, string state)
{
    var line = $"{tsUtc:o},{subId},{rg},{name},{state}\n";
    return File.AppendAllTextAsync(path, line);
}
