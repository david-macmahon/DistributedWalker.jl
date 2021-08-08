# DistributedWalker

This package implements a framework for recursively finding files in (typically
local) subdirectories of systems in a cluster and then "processing" them by
a supplied function.  By default, all files are processed, but the caller can
also provide a function to limit processing to only files for which the function
returns true.

This package is intended for use on clusters where the computers have local
filesystems mounted on a common mountpoint (e.g. `/datax`).  Having multiple
computers walking a common shared directory will result in duplicate work.
Currently, the list of top level directories to walk is used across all systems.
A future version may add more flexibility in this regard.

## Terminology

### Processes

Julia's built-in distributed computing support is based on starting multiple
Julia processes that can communicate between themselves.  The initial Julia
process, referred to here as the "driver process", has an ID of 1.  The driver
process is responsible for starting additional processes on the same and/or
other hosts and wiring up communications between the processes.  This can be
done via the command line with the `--procs` or `--machine-file` options or by
calling the `addprocs` function.  These additional processes are known as
"worker" processes.  Using multiple worker processes per node is supported and
the increased parallelization can improve performance.

### Jobs

`DistributedWalker` uses `RemoteChannel`s to communicate between processes.
Each host running worker processes gets its own `RemoteChannel` for posting
"jobs".  If the host has more than one process, then the `RemoteChannel` is
backed by an `AbstractChannel` on the process with the lowest ID of all
processes on that host.  This process is called the "leader" process.  Each
"job" posted to this channel is simply the absolute filename of a file that was
encountered during the recursive search for which the caller supplied filter
function returned `true`.  The leader process starts the file search and job
posting task asynchronously, then proceeds to process the jobs.  Non-leader
processes on the same host will only process jobs.  When the job posting task
completes, it posts "empty" jobs to the jobs channel, one for each process on
the same host, to indicate to the workers that there are no more jobs.

### Results

Worker processes take jobs (i.e. filenames) from the jobs channel and pass them
to the caller supplied `work` function.  The return value from the `work`
function is the "result" value for the "job".  The `work` function can return
`nothing` to indicate that no results are desired/needed for the job/file.  For
each non-nothing result, the worker process will post a results `Tuple` to the
`results` channel, which is a RemoteChannel backed by a Channel on the driver
process (with ID of 1).  The results `Tuple` has four elements: hostname,
process ID, filename, and the non-nothing results value returned from the `work`
function.

## Example

This example will return the sum of file sizes for all `*.h5` files in/under the
`/datax` directories of all hosts with worker processes.

```julia
# If adding procs from within the main process
using Distributed
addprocs(...)

# Make DistributedWalker available on all procs
@everywhere import DistributedWalker

# Launch DistributedWalker.  The `filesize` function is used to get the
# "results" for files whose names end with ".h5".  The "/datax" directory tree
# is walked on each host running a proc.
#
#The returned `resultschannel` is a RemoteChannel from which one can `take!`
#results, which are Tuples containing:

#    (hostname, id, # filename, results)
#
# An empty result (i.e. filename=="" and results===nothing) from a process
# indicates that the process has completed.  After receiving one such empty
# result from each process, all results have been received and futher `take!`
# calls on `resultschannel` will block forever.
resultschannel = DistributedWalker.launch(filesize, endswith(".h5"), "/datax")

# Use `getresults` to read all results from the channel and add up all the
# filesizes.  By default, `getresults` ends after reading `nworkers()` empty
# results.
totalsize = 0
DistributedWalker.getresults(results) do hostname, id, filename, size
    totalsize += size
end
println("total size is $totalsize")
```
