using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Spectre.Console;
using Spectre.Console.Rendering;

class Program
{
    static async Task Main(string[] args)
    {
        int workerCount = args.Length > 0 && int.TryParse(args[0], out var n) && n > 0 ? n : 4;
        string[] urls = File.Exists("urls.txt")
            ? File.ReadAllLines("urls.txt").Where(l => !string.IsNullOrWhiteSpace(l)).Select(l => l.Trim()).ToArray()
            : new[] { "http://ipv4.download.thinkbroadband.com/20MB.zip", "http://ipv4.download.thinkbroadband.com/50MB.zip" };

        var mgr = new DownloadManager(workerCount, urls, "downloads");
        var ui = new Renderer(mgr);
        bool quit = false;

        await AnsiConsole.Live(ui.Build()).AutoClear(false).StartAsync(async ctx =>
        {
            while (!quit)
            {
                // Non-blocking key polling: the UI loop never waits on workers.
                while (Console.KeyAvailable)
                {
                    switch (Console.ReadKey(true).Key)
                    {
                        case ConsoleKey.S: mgr.Start(); break;
                        case ConsoleKey.P: mgr.Pause(); break;
                        case ConsoleKey.R: mgr.Resume(); break;
                        case ConsoleKey.X: mgr.Stop(); break;
                        case ConsoleKey.Q: quit = true; break;
                    }
                }
                ctx.UpdateTarget(ui.Build());
                await Task.Delay(200);
            }
            await mgr.StopAndWaitAsync();
        });
    }
}

// ---------------------------------------------------------------- models

record Job(int Id, string Url);
record DownloadResult(int JobId, string File, long Size, TimeSpan Time, bool Ok, string Info);

class WorkerStatus
{
    public int Id;
    public volatile string State = "Idle";
    public volatile string File = "-";
    public long BytesDone;      // current file
    public long TotalBytes;     // current file (0 = unknown)
    public long LifetimeBytes;  // all files, used for speed
}

// Async-friendly pause gate: workers await it between chunks.
class PauseGate
{
    private readonly object _lock = new();
    private TaskCompletionSource _tcs = Open();
    private static TaskCompletionSource Open()
    {
        var t = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        t.SetResult();
        return t;
    }
    public bool IsPaused { get { lock (_lock) return !_tcs.Task.IsCompleted; } }
    public void Pause()
    {
        lock (_lock)
            if (_tcs.Task.IsCompleted)
                _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    public void Resume() { lock (_lock) _tcs.TrySetResult(); }
    public Task WaitAsync(CancellationToken ct)
    {
        Task t; lock (_lock) t = _tcs.Task;
        return t.IsCompleted ? Task.CompletedTask : t.WaitAsync(ct);
    }
}

// ---------------------------------------------------------------- manager

class DownloadManager
{
    private static readonly HttpClient Http = CreateClient();

    private static HttpClient CreateClient()
    {
        var c = new HttpClient { Timeout = TimeSpan.FromMinutes(30) };
        c.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Windows NT 10.0; Win64; x64) DownloaderDemo/1.0");
        c.DefaultRequestHeaders.Accept.ParseAdd("*/*");
        return c;
    }
    private readonly string[] _urls;
    private readonly string _dir;
    private readonly PauseGate _gate = new();
    private CancellationTokenSource? _cts;
    private Task[] _workers = Array.Empty<Task>();
    private long _totalBytes;

    public WorkerStatus[] Workers { get; }
    public ConcurrentQueue<DownloadResult> Results { get; } = new();
    public volatile string State = "Ready";
    public int JobCount => _urls.Length;
    public int Completed;   // updated with Interlocked
    public int Failed;
    public long TotalBytes => Interlocked.Read(ref _totalBytes);

    public DownloadManager(int workers, string[] urls, string dir)
    {
        _urls = urls; _dir = dir;
        Workers = Enumerable.Range(0, workers).Select(i => new WorkerStatus { Id = i + 1 }).ToArray();
        Directory.CreateDirectory(dir);
    }

    public void Start()
    {
        if (State == "Running" || State == "Paused") return;
        Results.Clear(); Completed = 0; Failed = 0; Interlocked.Exchange(ref _totalBytes, 0);
        _gate.Resume();
        _cts = new CancellationTokenSource();
        var token = _cts.Token;

        var channel = Channel.CreateUnbounded<Job>();
        for (int i = 0; i < _urls.Length; i++) channel.Writer.TryWrite(new Job(i + 1, _urls[i]));
        channel.Writer.Complete();

        State = "Running";
        _workers = Workers.Select(w => Task.Run(() => WorkerLoop(w, channel.Reader, token))).ToArray();
        _ = Task.WhenAll(_workers).ContinueWith(_ =>
        {
            if (!token.IsCancellationRequested) State = "Finished";
        });
    }

    public void Pause() { if (State == "Running") { _gate.Pause(); State = "Paused"; } }
    public void Resume() { if (State == "Paused") { _gate.Resume(); State = "Running"; } }
    public void Stop()
    {
        if (State is "Running" or "Paused") { State = "Stopped"; _cts?.Cancel(); _gate.Resume(); }
    }
    public async Task StopAndWaitAsync()
    {
        Stop();
        try { await Task.WhenAll(_workers); } catch { }
    }

    private async Task WorkerLoop(WorkerStatus st, ChannelReader<Job> reader, CancellationToken ct)
    {
        try
        {
            await foreach (var job in reader.ReadAllAsync(ct))
                await DownloadOne(st, job, ct);
            st.State = "Idle"; st.File = "-";
        }
        catch (OperationCanceledException) { st.State = "Stopped"; st.File = "-"; }
    }

    private async Task DownloadOne(WorkerStatus st, Job job, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        string path = "";
        string name = "download";
        try
        {
            var uri = new Uri(job.Url);
            name = Path.GetFileName(uri.LocalPath);
            if (string.IsNullOrWhiteSpace(name)) name = "download";
            st.File = name; st.State = "Downloading";
            Interlocked.Exchange(ref st.BytesDone, 0);
            Interlocked.Exchange(ref st.TotalBytes, 0);

            using var resp = await Http.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead, ct);
            resp.EnsureSuccessStatusCode();
            Interlocked.Exchange(ref st.TotalBytes, resp.Content.Headers.ContentLength ?? 0);

            path = Path.Combine(_dir, $"{job.Id}_{name}");   // job id avoids name collisions
            await using var src = await resp.Content.ReadAsStreamAsync(ct);
            await using (var dst = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, true))
            {
                var buf = new byte[81920];
                int read;
                while (true)
                {
                    await _gate.WaitAsync(ct);                       // pause point (every chunk)
                    read = await src.ReadAsync(buf, ct);
                    if (read == 0) break;
                    await dst.WriteAsync(buf.AsMemory(0, read), ct);
                    Interlocked.Add(ref st.BytesDone, read);
                    Interlocked.Add(ref st.LifetimeBytes, read);
                    Interlocked.Add(ref _totalBytes, read);
                }
            }
            Interlocked.Increment(ref Completed);
            Results.Enqueue(new DownloadResult(job.Id, name, Interlocked.Read(ref st.BytesDone), sw.Elapsed, true, path));
        }
        catch (OperationCanceledException)
        {
            if (path != "") try { File.Delete(path); } catch { }    // remove partial file
            throw;
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref Failed);
            Results.Enqueue(new DownloadResult(job.Id, name, 0, sw.Elapsed, false, ex.Message));
        }
        finally { if (st.State == "Downloading") st.State = "Idle"; }
    }
}

// ---------------------------------------------------------------- UI

class Renderer
{
    private readonly DownloadManager _m;
    private readonly long[] _prev;
    private readonly double[] _speed;
    private long _prevTotal; private double _totalSpeed;
    private readonly Stopwatch _tick = Stopwatch.StartNew();

    public Renderer(DownloadManager m)
    {
        _m = m; _prev = new long[m.Workers.Length]; _speed = new double[m.Workers.Length];
    }

    private static string Bar(double frac, int w = 20)
    {
        int f = (int)Math.Round(Math.Clamp(frac, 0, 1) * w);
        return "[green]" + new string('█', f) + "[/][grey]" + new string('░', w - f) + "[/]";
    }
    private static string Mb(long b) => $"{b / 1048576.0:F1} MB";

    public IRenderable Build()
    {
        // speed sampling on each UI tick (smoothed)
        double dt = Math.Max(_tick.Elapsed.TotalSeconds, 0.001); _tick.Restart();
        for (int i = 0; i < _m.Workers.Length; i++)
        {
            long cur = Interlocked.Read(ref _m.Workers[i].LifetimeBytes);
            double inst = (cur - _prev[i]) / dt; _prev[i] = cur;
            _speed[i] = _speed[i] * 0.6 + inst * 0.4;
        }
        long tot = _m.TotalBytes;
        _totalSpeed = _totalSpeed * 0.6 + ((tot - _prevTotal) / dt) * 0.4; _prevTotal = tot;

        var stateColor = _m.State switch { "Running" => "green", "Paused" => "yellow", "Stopped" => "red", "Finished" => "blue", _ => "grey" };
        var header = new Markup(
            $"[bold]Multi-threaded Downloader[/]  |  State: [{stateColor}]{_m.State}[/]  |  Workers: {_m.Workers.Length}\n" +
            $"Done: [green]{_m.Completed}[/]  Failed: [red]{_m.Failed}[/]  Total jobs: {_m.JobCount}  |  " +
            $"Downloaded: {Mb(tot)}  |  Speed: [cyan]{_totalSpeed / 1048576.0:F2} MB/s[/]\n");

        var wt = new Table().Border(TableBorder.Rounded).Title("Workers")
            .AddColumns("#", "State", "File", "Progress", "Done", "Speed");
        for (int i = 0; i < _m.Workers.Length; i++)
        {
            var w = _m.Workers[i];
            long done = Interlocked.Read(ref w.BytesDone), total = Interlocked.Read(ref w.TotalBytes);
            string state = w.State;
            if (state == "Downloading" && _m.State == "Paused") state = "Paused";
            var c = state switch { "Downloading" => "green", "Paused" => "yellow", "Stopped" => "red", _ => "grey" };
            bool active = w.State == "Downloading";
            string prog = active
                ? (total > 0 ? $"{Bar((double)done / total)} {100.0 * done / total,3:F0}%" : "[grey]size unknown[/]")
                : "";
            wt.AddRow(w.Id.ToString(), $"[{c}]{state}[/]", Markup.Escape(w.File), prog,
                      active ? Mb(done) : "", state == "Downloading" ? $"{_speed[i] / 1048576.0:F2} MB/s" : "");
        }

        var rt = new Table().Border(TableBorder.Rounded).Title("Results (latest 8)")
            .AddColumns("Job", "File", "Status", "Size", "Time", "Info");
        foreach (var r in _m.Results.ToArray().TakeLast(8))
            rt.AddRow(r.JobId.ToString(), Markup.Escape(r.File),
                      r.Ok ? "[green]OK[/]" : "[red]FAILED[/]",
                      r.Ok ? Mb(r.Size) : "-", $"{r.Time.TotalSeconds:F1}s",
                      Markup.Escape(r.Info.Length > 50 ? "..." + r.Info[^47..] : r.Info));

        var help = new Markup("[grey][[S]] Start  [[P]] Pause  [[R]] Resume  [[X]] Stop  [[Q]] Quit[/]");
        return new Rows(header, wt, rt, help);
    }
}