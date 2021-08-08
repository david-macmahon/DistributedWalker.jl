module DistributedWalker

using Distributed

"""
Return the id of the leader process of the node running process `id`.
The process with the lowest id of all workers on a given host is that host's
"leader" process.  Note that the process with id==1 is not considered a worker
process and is never considered a leader process.  Passing 1 for `id` will
throw and exception unless its node has worker processes.
"""
function leader(id::Integer)
    peers = procs(id)
    @assert maximum(peers) != 1 "no workers for main proc"
    minimum(setdiff!(peers, 1))
end

"""
Return a list of all leader processes (one per node).
"""
function leaders()
    @assert nprocs() > 1 "no workers for main proc"
    unique(leader.(workers()))
end

"""
Return a Dict mapping leader ids to RemoteChannels that "live" in the leader
procs.
"""
function makejobchannels(len=1000)
    # TODO Parameterize this function to allow for different Channel types
    [l => RemoteChannel(()->Channel{String}(len), l) for l in leaders()] |> Dict
end

"""
Walk `topdirs` and puts all absolute pathnames for which `predicate` returns
`true` into RemoteChannel `jobs`. The caller supplied `predicate` function is
passed a single absolute path name argument and is expected to return a Bool.

When the walk of `topdirs` has completed, one empty string is put into the
`jobs` channel for each process running on the same node as the (leader) proc
calling this function.
"""
function makejobs(predicate::Function, jobs::RemoteChannel, topdir)
    @assert myid() == leader(myid()) "only leaders can make jobs"
    @debug "$(gethostname()): walking $topdir"
    # TODO Don't require/assume topdir to be a directory
    for (base, _, files) in walkdir(topdir, onerror=err->nothing)
        @debug "$(gethostname()): making jobs for files in $base"
        for fn in files
            absname = abspath(base, fn)
            try
                predicate(absname) && put!(jobs, absname)
            catch ex
                msgbuf = IOBuffer()
                showerror(msgbuf, ex)
                msg = String(take!(msgbuf))
                @error """$(gethostname())[$(myid())]: predicate("$absname") got $msg""" _file=nothing _module=nothing
            end
        end
    end

    # put! as many empty jobs in channel as there are workers on current node
    for _ in setdiff(procs(myid()), 1)
        put!(jobs, "")
    end
end

"""
Work jobs (i.e. filenames) from the `jobs` channel and post results to the
`results` channel.  Leader processes will also start an asynchronous task for
each directory in `topdirs` that will walk the files in/under the directory and
post to the `jobs` channel filenames for which `predicate` returns true.

`topdirs` can be a Vector of AbstractStrings or a single AbstractString to walk
just a single direcrory.
"""
function workjobs(work::Function, predicate::Function,
                  jobs::RemoteChannel, results::RemoteChannel,
                  topdirs::AbstractVector{<:AbstractString})
    # Who/where am I?
    id = myid()
    hostname = gethostname()

    # The leader process also makes jobs
    if myid() == leader(myid())
        for topdir in topdirs
            @async makejobs(predicate, jobs, topdir)
        end
    end

    ## Supress HDF5 error messages
    #olderrhandler = HDF5.h5e_get_auto(HDF5.H5E_DEFAULT)
    #HDF5.h5e_set_auto(HDF5.H5E_DEFAULT, C_NULL, C_NULL)

    # Take jobs (i.e. filename) until we get an empty job
    while (job=take!(jobs); !isempty(job))
        try
            result = work(job)
            result === nothing || put!(results, (hostname, id, job, result))
        catch ex
            msgbuf = IOBuffer()
            showerror(msgbuf, ex)
            msg = String(take!(msgbuf))
            @error """$hostname[$id]: work("$job") got $msg""" _file=nothing _module=nothing
        end
        #status = "noth5x"
        #try
        #    if HDF5.ishdf5(fname) && ish5x(fname)
        #        polbug = haspolbug(fname)
        #        status = polbug ? "polbug" : "ok"
        #    end
        #catch
        #    status = "corrupt"
        #end
        #put!(results, (hostname, id, fname, status))
    end

    ## Restore HDF5 error handling
    #HDF5.h5e_set_auto(HDF5.H5E_DEFAULT, olderrhandler...)

    # Put empty result into results channel
    put!(results, (hostname, id, "", nothing))
end

"""
Launch a distribted walk of `topdirs` using all processes in `procs`.  Return a
`DistributedChannel` from which results can be taken.

`predicate` is a function that should accept a single AbstractString parameter
and return a `Bool`.  The passing in value will be a filename.  A return value
of `true` means the filename should be posted to the jobs channel.  If
`predicate` is not given, it defaults to `!islink` which selects all files that
are not symbolic links.

`work` is a function that takes a filename and returns some sort of result.  If
the result is not `nothing`, a result Tuple will be posted to the results
channel.  The result Tuple has four fields: hostname, id, filename, and result.

`topdirs` must be a Vector of AbstractStrings containing the top level
directoris to walk or an AbstractString to walk just a single top level
directory.

`procs` gives the worker processes to be used.  Use a subset of workers other
than the default of all worker processes at your own risk (i.e. don't).

`jobchansize` and `resultschansize` are used for the sizes of job and results
channels.
"""
function launch(work::Function, predicate::Function, topdirs::AbstractVector;
                procs=workers(), jobchansize=1000, resultschansize=1000)
    jobchans = makejobchannels(jobchansize)
    results = RemoteChannel(()->Channel{Tuple}(resultschansize), 1)
    for proc in procs
        remote_do(workjobs, proc, predicate, work, jobchans[leader(proc)], results, topdirs)
    end
    results
end

# Default predicate version
function launch(work::Function, topdirs::AbstractVector;
                procs=workers(), jobchansize=1000, resultschansize=1000)
    launch(work, !islink, topdirs;
           procs=procs, jobchansize=jobchansize, resultschansize=resultschansize)
end

# topdir::AbstractString version
function launch(work::Function, predicate::Function, topdir::AbstractString;
                procs=workers(), jobchansize=1000, resultschansize=1000)
    launch(work, predicate, [topdir];
           procs=procs, jobchansize=jobchansize, resultschansize=resultschansize)
end

# Default predicate topdir::AbstractString version
function launch(work::Function, topdir::AbstractString;
                procs=workers(), jobchansize=1000, resultschansize=1000)
    launch(work, !islink, [topdir];
           procs=procs, jobchansize=jobchansize, resultschansize=resultschansize)
end

"""
Take results from the `results` channel and pass each one to `f` untl receiving
`npending` empty results.  If `f` is not given, it defaults to `println`.
"""
function getresults(f::Function, results, npending=nworkers())
    while npending > 0
        result = take!(results)
        if result[4] === nothing
            npending -= 1
        else
            f(result)
        end
    end
end

function getresults(results, npending=nworkers())
    getresults(println, results, npending)
end

end # module DistributedWalker
