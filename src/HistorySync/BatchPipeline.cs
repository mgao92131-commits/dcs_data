using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

namespace DeltaVHistoryCLI
{
    enum BatchWorkState
    {
        Prepared,
        Sending,
        Acked,
        Failed
    }

    sealed class PreparedBatch
    {
        public long Sequence;
        public HistoryBatch Batch;
        public BatchPayload Payload;
        public DateTime RangeStart;
        public DateTime RangeEnd;
        public BatchWorkState State;
        public BatchReceipt Receipt;
        public Exception Error;
        public int ResultCode;
        public long HistorianReadMilliseconds;
        public long EncodeMilliseconds;
        public Stopwatch TotalClock;
    }

    class BatchPipeline : IDisposable
    {
        private const int PipelineDepth = 2;
        private readonly SyncOptions _options;
        private readonly BatchSender _sender;
        private readonly SyncState _state;
        private readonly SyncStateStore _stateStore;
        private readonly SyncLogger _log;
        private readonly WaitHandle _externalStop;
        private readonly ManualResetEvent _pipelineStop =
            new ManualResetEvent(false);
        private readonly WaitHandle[] _workerStopHandles;
        private readonly object _gate = new object();
        private readonly object _checkpointGate = new object();
        private readonly Queue<PreparedBatch> _ready =
            new Queue<PreparedBatch>();
        private readonly List<PreparedBatch> _active =
            new List<PreparedBatch>();
        private readonly Thread[] _workers = new Thread[PipelineDepth];
        private long _nextSequence = 1;
        private long _nextCheckpointSequence = 1;
        private bool _checkpointEnabled;
        private bool _producerCompleted;
        private bool _stopRequested;
        private bool _workersJoined;
        private bool _finished;
        private bool _disposed;
        private Exception _fatalError;

        public BatchPipeline(
            SyncOptions options,
            BatchSender sender,
            SyncState state,
            SyncStateStore stateStore,
            SyncLogger log,
            WaitHandle externalStop)
        {
            if (options == null)
                throw new ArgumentNullException("options");
            if (sender == null)
                throw new ArgumentNullException("sender");
            if (log == null)
                throw new ArgumentNullException("log");
            if (state != null && stateStore == null)
                throw new ArgumentNullException("stateStore");

            _options = options;
            _sender = sender;
            _state = state;
            _stateStore = stateStore;
            _log = log;
            _externalStop = externalStop;
            _checkpointEnabled =
                String.Equals(options.Command, "sync", StringComparison.OrdinalIgnoreCase) &&
                state != null && stateStore != null;
            _workerStopHandles = externalStop == null
                ? new WaitHandle[] { _pipelineStop }
                : new WaitHandle[] { externalStop, _pipelineStop };

            int i;
            for (i = 0; i < _workers.Length; i++)
            {
                _workers[i] = new Thread(WorkerLoop);
                _workers[i].IsBackground = true;
                _workers[i].Name = "HistorySync-Sender-" +
                    (i + 1).ToString(CultureInfo.InvariantCulture);
                _workers[i].Start();
            }
        }

        public int InFlight
        {
            get
            {
                lock (_gate)
                    return _active.Count;
            }
        }

        public DateTime CheckpointEnd
        {
            get
            {
                lock (_checkpointGate)
                {
                    if (_state == null)
                        return DateTime.MinValue;
                    return _state.CheckpointEnd;
                }
            }
        }

        public void WaitForCapacity()
        {
            while (true)
            {
                lock (_gate)
                {
                    ThrowIfFatalLocked();
                    if (IsStopRequestedLocked())
                    {
                        RequestStopLocked();
                        throw new SyncStopRequestedException(
                            "Stop requested while waiting for pipeline capacity.");
                    }
                    if (_active.Count < PipelineDepth)
                        return;
                    Monitor.Wait(_gate, 100);
                }
            }
        }

        public long Submit(PreparedBatch prepared)
        {
            if (prepared == null)
                throw new ArgumentNullException("prepared");
            if (prepared.Batch == null)
                throw new ArgumentException("Prepared batch has no HistoryBatch.", "prepared");
            if (prepared.Payload == null)
                throw new ArgumentException("Prepared batch has no payload.", "prepared");

            int inFlight;
            long sequence;
            lock (_gate)
            {
                ThrowIfFatalLocked();
                if (IsStopRequestedLocked())
                {
                    RequestStopLocked();
                    throw new SyncStopRequestedException(
                        "Stop requested before submitting a prepared batch.");
                }
                if (_active.Count >= PipelineDepth)
                    throw new InvalidOperationException(
                        "Batch pipeline capacity must be checked before Submit.");
                sequence = _nextSequence++;
                prepared.Sequence = sequence;
                prepared.RangeStart = prepared.Batch.RangeStart;
                prepared.RangeEnd = prepared.Batch.RangeEnd;
                prepared.State = BatchWorkState.Prepared;
                if (prepared.TotalClock == null)
                    prepared.TotalClock = Stopwatch.StartNew();
                _active.Add(prepared);
                _ready.Enqueue(prepared);
                inFlight = _active.Count;
                Monitor.PulseAll(_gate);
            }
            _log.Write(
                "Submitted seq=" +
                sequence.ToString(CultureInfo.InvariantCulture) +
                " batch=" + prepared.Batch.BatchId +
                " inFlight=" + inFlight.ToString(CultureInfo.InvariantCulture) +
                " pipelineDepth=" + PipelineDepth.ToString(CultureInfo.InvariantCulture));
            return sequence;
        }

        public void AdvanceCheckpoint()
        {
            lock (_checkpointGate)
            {
                if (!_checkpointEnabled)
                {
                    RemoveAcknowledgedWithoutCheckpoint();
                    return;
                }

                while (true)
                {
                    PreparedBatch next = FindNextAcknowledged();
                    if (next == null)
                        return;

                    try
                    {
                        SyncProgram.SaveCheckpointWithRetry(
                            _state,
                            _stateStore,
                            next.RangeEnd,
                            _log,
                            null);
                    }
                    catch (Exception ex)
                    {
                        SetFatal(ex);
                        throw;
                    }

                    int inFlight;
                    lock (_gate)
                    {
                        if (!_active.Remove(next))
                            return;
                        _nextCheckpointSequence = next.Sequence + 1;
                        inFlight = _active.Count;
                        Monitor.PulseAll(_gate);
                    }
                    _log.Write(
                        "Checkpoint advanced seq=" +
                        next.Sequence.ToString(CultureInfo.InvariantCulture) +
                        " end=" + SyncProgram.FormatTime(next.RangeEnd) +
                        " inFlight=" + inFlight.ToString(CultureInfo.InvariantCulture) +
                        " pipelineDepth=" + PipelineDepth.ToString(CultureInfo.InvariantCulture));
                }
            }
        }

        public void WaitForAll()
        {
            bool stopping = false;
            lock (_gate)
            {
                while (true)
                {
                    if (_fatalError != null || IsStopRequestedLocked())
                    {
                        RequestStopLocked();
                        stopping = true;
                        break;
                    }
                    if (_active.Count == 0)
                    {
                        _producerCompleted = true;
                        Monitor.PulseAll(_gate);
                        break;
                    }
                    Monitor.Wait(_gate, 100);
                }
            }

            JoinWorkers();
            Exception fatal = null;
            try
            {
                AdvanceCheckpoint();
            }
            catch (Exception ex)
            {
                fatal = ex;
                SetFatal(ex);
            }

            bool stopped;
            lock (_gate)
            {
                if (fatal == null)
                    fatal = _fatalError;
                stopped = _stopRequested || IsExternalStopRequested();
                DiscardUncommittedLocked();
                _producerCompleted = true;
                _finished = true;
                Monitor.PulseAll(_gate);
            }

            if (fatal != null)
                throw fatal;
            if (stopping || stopped)
                throw new SyncStopRequestedException(
                    "Stop requested while draining the batch pipeline.");
        }

        public void Stop()
        {
            lock (_gate)
                RequestStopLocked();
        }

        public SyncState CopyCheckpointState()
        {
            lock (_checkpointGate)
            {
                if (_state == null)
                    return null;
                return _state.Copy();
            }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                if (_disposed)
                    return;
                if (!_finished)
                    RequestStopLocked();
            }

            JoinWorkers();
            if (!_finished)
            {
                try
                {
                    AdvanceCheckpoint();
                }
                catch (Exception ex)
                {
                    SetFatal(ex);
                }
                lock (_gate)
                {
                    DiscardUncommittedLocked();
                    _producerCompleted = true;
                    _finished = true;
                    Monitor.PulseAll(_gate);
                }
            }
            _pipelineStop.Close();
            _disposed = true;
        }

        private void WorkerLoop()
        {
            while (true)
            {
                PreparedBatch prepared = Dequeue();
                if (prepared == null)
                    return;

                try
                {
                    BatchReceipt receipt = _sender.SendWithRetryAny(
                        prepared.Batch,
                        prepared.Payload,
                        _workerStopHandles);
                    bool waitingForEarlier = false;
                    long waitingForSequence = 0;
                    lock (_gate)
                    {
                        prepared.Receipt = receipt;
                        prepared.State = BatchWorkState.Acked;
                        if (_checkpointEnabled &&
                            prepared.Sequence > _nextCheckpointSequence)
                        {
                            waitingForEarlier = true;
                            waitingForSequence = _nextCheckpointSequence;
                        }
                        Monitor.PulseAll(_gate);
                    }

                    _log.Write(
                        "ACK seq=" +
                        prepared.Sequence.ToString(CultureInfo.InvariantCulture) +
                        " batch=" + prepared.Batch.BatchId +
                        " range=" + SyncProgram.FormatTime(prepared.RangeStart) +
                        " .. " + SyncProgram.FormatTime(prepared.RangeEnd));
                    if (waitingForEarlier)
                        _log.Write(
                            "ACK seq=" +
                            prepared.Sequence.ToString(CultureInfo.InvariantCulture) +
                            " waiting_for_seq=" +
                            waitingForSequence.ToString(CultureInfo.InvariantCulture));
                    AdvanceCheckpoint();
                    if (prepared.TotalClock != null)
                        prepared.TotalClock.Stop();
                    LogPreparedBatch(prepared, receipt);
                }
                catch (SyncStopRequestedException ex)
                {
                    MarkFailed(prepared, ex);
                    Stop();
                    return;
                }
                catch (BatchSendException ex)
                {
                    MarkFailed(prepared, ex);
                    SetFatal(ex);
                    TryAdvanceAfterFailure();
                    return;
                }
                catch (Exception ex)
                {
                    MarkFailed(prepared, ex);
                    if (!IsExternalStopRequested())
                        SetFatal(ex);
                    else
                        Stop();
                    TryAdvanceAfterFailure();
                    return;
                }
            }
        }

        private PreparedBatch Dequeue()
        {
            lock (_gate)
            {
                while (_ready.Count == 0 &&
                    !_producerCompleted &&
                    !_stopRequested)
                    Monitor.Wait(_gate, 100);
                if (_ready.Count == 0 || _stopRequested)
                    return null;

                PreparedBatch prepared = _ready.Dequeue();
                prepared.State = BatchWorkState.Sending;
                Monitor.PulseAll(_gate);
                return prepared;
            }
        }

        private void LogPreparedBatch(
            PreparedBatch prepared,
            BatchReceipt receipt)
        {
            SyncState state = CopyCheckpointState();
            int inFlight = InFlight;
            SyncProgram.LogBatchMetrics(
                _options,
                state,
                _log,
                prepared.Batch,
                prepared.Payload.Length,
                prepared.TotalClock,
                prepared.HistorianReadMilliseconds,
                prepared.EncodeMilliseconds,
                receipt == null ? null : receipt.Timings,
                prepared.Sequence,
                PipelineDepth,
                inFlight);
        }

        private void MarkFailed(PreparedBatch prepared, Exception error)
        {
            lock (_gate)
            {
                prepared.Error = error;
                prepared.State = BatchWorkState.Failed;
                Monitor.PulseAll(_gate);
            }
            _log.Write(
                "Pipeline batch failed seq=" +
                prepared.Sequence.ToString(CultureInfo.InvariantCulture) +
                " batch=" + prepared.Batch.BatchId +
                " error=" + error.Message);
        }

        private void SetFatal(Exception error)
        {
            lock (_gate)
            {
                if (_fatalError == null && !IsExternalStopRequested())
                    _fatalError = error;
                RequestStopLocked();
            }
            _log.Write("Pipeline fatal error: " + error.Message);
        }

        private void TryAdvanceAfterFailure()
        {
            try
            {
                AdvanceCheckpoint();
            }
            catch (Exception ex)
            {
                SetFatal(ex);
            }
        }

        private PreparedBatch FindNextAcknowledged()
        {
            lock (_gate)
            {
                int i;
                for (i = 0; i < _active.Count; i++)
                {
                    PreparedBatch prepared = _active[i];
                    if (prepared.Sequence == _nextCheckpointSequence)
                        return prepared.State == BatchWorkState.Acked
                            ? prepared
                            : null;
                }
                return null;
            }
        }

        private void RemoveAcknowledgedWithoutCheckpoint()
        {
            lock (_gate)
            {
                int i = _active.Count - 1;
                while (i >= 0)
                {
                    if (_active[i].State == BatchWorkState.Acked)
                        _active.RemoveAt(i);
                    i--;
                }
                Monitor.PulseAll(_gate);
            }
        }

        private void DiscardUncommittedLocked()
        {
            int discarded = _active.Count;
            _ready.Clear();
            _active.Clear();
            if (discarded > 0)
                _log.Write(
                    "Discarded uncheckpointed pipeline batches=" +
                    discarded.ToString(CultureInfo.InvariantCulture));
        }

        private void JoinWorkers()
        {
            lock (_gate)
            {
                if (_workersJoined)
                    return;
            }
            int i;
            for (i = 0; i < _workers.Length; i++)
            {
                if (_workers[i] != null &&
                    Thread.CurrentThread != _workers[i])
                    _workers[i].Join();
            }
            lock (_gate)
                _workersJoined = true;
        }

        private bool IsExternalStopRequested()
        {
            return _externalStop != null && _externalStop.WaitOne(0, false);
        }

        private bool IsStopRequestedLocked()
        {
            return _stopRequested || IsExternalStopRequested();
        }

        private void RequestStopLocked()
        {
            _stopRequested = true;
            _pipelineStop.Set();
            Monitor.PulseAll(_gate);
        }

        private void ThrowIfFatalLocked()
        {
            if (_fatalError != null)
                throw _fatalError;
        }
    }
}
