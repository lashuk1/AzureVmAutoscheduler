using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure.Identity;
using Azure.ResourceManager;
using Azure.ResourceManager.Compute;
using Azure.ResourceManager.Resources;

#region Config
string CsvPath = Env("CSV_PATH", "vm-log.csv");
int PollMinutes = Math.Max(1, Int("POLL_MINUTES", 5));
int MaxDop = Math.Max(1, Int("MAX_DOP", 8));
double RunningLimitHours = DoubleInv("RUNNING_LIMIT_HOURS", 8.0);
bool DryRun = Bool("DRY_RUN", false);

static string Env(string k, string d) => Environment.GetEnvironmentVariable(k) ?? d;
static int Int(string k, int d) => int.TryParse(Env(k, ""), out var v) ? v : d;
static double DoubleInv(string k, double d)
    => double.TryParse(Env(k, ""), NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : d;
static bool Bool(string k, bool d)
    => bool.TryParse(Env(k, ""), out var v) ? v : d;
#endregion

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

Console.WriteLine($"{Now()} Azure VM Autoscheduler starting…");
Console.WriteLine($"{Now()} CSV_PATH={CsvPath}, POLL_MINUTES={PollMinutes}, MAX_DOP={MaxDop}, RUNNING_LIMIT_HOURS={RunningLimitHours}, DRY_RUN={DryRun}");

await EnsureCsvHeaderAsync(CsvPath, cts.Token);

var firstSeenRunningUtc = new ConcurrentDictionary<string, DateTimeOffset>(StringComparer.OrdinalIgnoreCase);
var csvLock = new SemaphoreSlim(1, 1);

var credential = new DefaultAzureCredential();
var arm = new ArmClient(credential);

var timer = new PeriodicTimer(TimeSpan.FromMinutes(PollMinutes));
while (true)
{
    try
    {
        await PollOnceAsync(arm, CsvPath, firstSeenRunningUtc, csvLock, RunningLimitHours, DryRun, MaxDop, cts.Token);
    }
    catch (OperationCanceledException) when (cts.IsCancellationRequested)
    {
        Console.WriteLine($"{Now()} Cancellation requested. Exiting.");
        break;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"{Now()} ERROR (top-level): {ex}");
    }

    try
    {
        await timer.WaitForNextTickAsync(cts.Token);
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine($"{Now()} Stopped.");
        break;
    }
}

async Task PollOnceAsync(
    ArmClient armClient,
    string csvPath,
    ConcurrentDictionary<string, DateTimeOffset> firstSeenRunning,
    SemaphoreSlim csvLock,
    double runningLimitHours,
    bool dryRun,
    int maxDop,
    CancellationToken ct)
{
    Console.WriteLine($"{Now()} Polling…");

    var subs = armClient.GetSubscriptions().GetAllAsync();

    var subGate = new SemaphoreSlim(maxDop);
    var subTasks = new List<Task>();

    await foreach (var sub in subs)
    {
        await subGate.WaitAsync(ct);
        subTasks.Add(Task.Run(async () =>
        {
            try
            {
                await ProcessSubscriptionAsync(sub, csvPath, firstSeenRunning, csvLock, runningLimitHours, dryRun, maxDop, ct);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{Now()} [Sub:{sub.Data.SubscriptionId}] ERROR: {ex.Message}");
            }
            finally
            {
                subGate.Release();
            }
        }));
    }

    await Task.WhenAll(subTasks);
}

async Task ProcessSubscriptionAsync(
    SubscriptionResource subscription,
    string csvPath,
    ConcurrentDictionary<string, DateTimeOffset> firstSeenRunning,
    SemaphoreSlim csvLock,
    double runningLimitHours,
    bool dryRun,
    int maxDop,
    CancellationToken ct)
{
    Console.WriteLine($"{Now()} Subscription: {subscription.Data.DisplayName} ({subscription.Data.SubscriptionId})");

    var vmGate = new SemaphoreSlim(maxDop);
    var vmTasks = new List<Task>();

    var rgCollection = subscription.GetResourceGroups();
    await foreach (var rg in rgCollection.GetAllAsync())
    {
        var vmCollection = rg.GetVirtualMachines();
        await foreach (var vm in vmCollection.GetAllAsync())
        {
            await vmGate.WaitAsync(ct);
            vmTasks.Add(Task.Run(async () =>
            {
                try
                {
                    await ProcessVmAsync(vm, csvPath, firstSeenRunning, csvLock, runningLimitHours, dryRun, ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{Now()} [VM:{vm.Id}] ERROR: {ex.Message}");
                }
                finally
                {
                    vmGate.Release();
                }
            }));
        }
    }
    await Task.WhenAll(vmTasks);
}

async Task ProcessVmAsync(
    VirtualMachineResource vm,
    string csvPath,
    ConcurrentDictionary<string, DateTimeOffset> firstSeenRunning,
    SemaphoreSlim csvLock,
    double runningLimitHours,
    bool dryRun,
    CancellationToken ct)
{
    var id = vm.Id;
    var subId = id.SubscriptionId ?? "";
    var rg = id.ResourceGroupName ?? "";
    var data = vm.Data;
    if (data is null)
    {
        Console.WriteLine($"{Now()} [VM:{id}] Missing Data");
        return;
    }
    var computerName = data.OSProfile?.ComputerName ?? data.Name ?? "";

    var hasAuto = data.Tags != null
        && data.Tags.TryGetValue("Autoshutdown", out var tagVal)
        && tagVal == "1";

    string powerDisplay = ""; 

    try
    {
        if (hasAuto)
        {
            var ivResp = await vm.InstanceViewAsync(ct);
            powerDisplay = ivResp.Value?.Statuses?
                .FirstOrDefault(s => s.Code != null && s.Code.StartsWith("PowerState/", StringComparison.OrdinalIgnoreCase))
                ?.DisplayStatus ?? "Unknown";

            var key = id.ToString();

            if (IsRunning(powerDisplay))
            {
                var now = DateTimeOffset.UtcNow;
                var first = firstSeenRunning.GetOrAdd(key, now);
                var runningFor = now - first;

                if (runningFor > TimeSpan.FromHours(runningLimitHours))
                {
                    Console.WriteLine($"{Now()} [{computerName}] Running {runningFor.TotalHours:F1}h > {runningLimitHours}h → PowerOff {(dryRun ? "(DRY RUN)" : "")}");
                    if (!dryRun)
                    {
                        try
                        {
                            await vm.PowerOffAsync(
                                waitUntil: Azure.WaitUntil.Completed,
                                skipShutdown: false,
                                cancellationToken: ct);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{Now()} [{computerName}] PowerOff failed: {ex.Message}");
                        }
                    }

                    firstSeenRunning.TryRemove(key, out _);
                }
            }
            else
            {
                firstSeenRunning.TryRemove(key, out _);
                
                if (IsStoppedAllocated(powerDisplay))
                {
                    Console.WriteLine($"{Now()} [{computerName}] Stopped (allocated) → Deallocate {(dryRun ? "(DRY RUN)" : "")}");
                    if (!dryRun)
                    {
                        try
                        {
                            await vm.DeallocateAsync(
                                waitUntil: Azure.WaitUntil.Completed,
                                cancellationToken: ct);
                            powerDisplay = "Deallocated";
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{Now()} [{computerName}] Deallocate failed: {ex.Message}");
                        }
                    }
                }
            }
        }
        else
        {
            firstSeenRunning.TryRemove(id.ToString(), out _);
        }
    }
    catch (OperationCanceledException) { throw; }
    catch (Exception exVm)
    {
        Console.WriteLine($"{Now()} [{computerName}] VM processing error: {exVm.Message}");
    }

    await AppendCsvAsync(csvPath, csvLock, new[]
    {
        DateTimeOffset.UtcNow.UtcDateTime.ToString("o", CultureInfo.InvariantCulture),
        subId,
        rg,
        computerName,
        powerDisplay 
    }, ct);
}

bool IsRunning(string display)
    => display.Contains("running", StringComparison.OrdinalIgnoreCase);

bool IsStoppedAllocated(string display)
    => display.Contains("stopped", StringComparison.OrdinalIgnoreCase)
       && !display.Contains("deallocated", StringComparison.OrdinalIgnoreCase);

async Task EnsureCsvHeaderAsync(string path, CancellationToken ct)
{
    if (!File.Exists(path))
    {
        await AppendCsvAsync(path, new SemaphoreSlim(1, 1), new[]
        {
            "TimestampUtc","SubscriptionId","ResourceGroup","ComputerName","PowerStateIfAutoShutdown"
        }, ct);
    }
}

async Task AppendCsvAsync(string path, SemaphoreSlim csvLock, string[] fields, CancellationToken ct)
{
    static string Esc(string v)
    {
        if (string.IsNullOrEmpty(v)) return "";
        var needsQuotes = v.Contains(',') || v.Contains('"') || v.Contains('\n') || v.Contains('\r');
        if (!needsQuotes) return v;
        return "\"" + v.Replace("\"", "\"\"") + "\"";
    }

    var line = string.Join(",", fields.Select(Esc)) + Environment.NewLine;

    await csvLock.WaitAsync(ct);
    try
    {
        await File.AppendAllTextAsync(path, line, new UTF8Encoding(false), ct);
    }
    finally
    {
        csvLock.Release();
    }
}

string Now() => $"[{DateTimeOffset.UtcNow:yyyy-MM-dd HH:mm:ss 'UTC'}]";
