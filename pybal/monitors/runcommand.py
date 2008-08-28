"""
runcommand.py
Copyright (C) 2008 by Mark Bergsma <mark@nedworks.org>

Monitor class implementations for PyBal

$Id$
"""

from pybal import monitor

import os, sys, signal, errno

from twisted.internet import reactor, process, error

class ProcessGroupProcess(process.Process):
    """
    Derivative of twisted.internet.process that supports Unix
    process groups, sessions and timeouts
    """
    
    def __init__(self,
                 reactor, command, args, environment, path, proto,
                 uid=None, gid=None, childFDs=None,
                 sessionLeader=False, timeout=None):
        """Spawn an operating-system process.

        This is where the hard work of disconnecting all currently open
        files / forking / executing the new process happens.  (This is
        executed automatically when a Process is instantiated.)

        This will also run the subprocess as a given user ID and group ID, if
        specified.  (Implementation Note: this doesn't support all the arcane
        nuances of setXXuid on UNIX: it will assume that either your effective
        or real UID is 0.)
        """
        if not proto:
            assert 'r' not in childFDs.values()
            assert 'w' not in childFDs.values()
        if not signal.getsignal(signal.SIGCHLD):
            log.msg("spawnProcess called, but the SIGCHLD handler is not "
                    "installed. This probably means you have not yet "
                    "called reactor.run, or called "
                    "reactor.run(installSignalHandler=0). You will probably "
                    "never see this process finish, and it may become a "
                    "zombie process.")
            # if you see this message during a unit test, look in
            # test-standard.xhtml or twisted.test.test_process.SignalMixin
            # for a workaround

        self.lostProcess = False

        settingUID = (uid is not None) or (gid is not None)
        if settingUID:
            curegid = os.getegid()
            currgid = os.getgid()
            cureuid = os.geteuid()
            curruid = os.getuid()
            if uid is None:
                uid = cureuid
            if gid is None:
                gid = curegid
            # prepare to change UID in subprocess
            os.setuid(0)
            os.setgid(0)

        self.pipes = {}
        # keys are childFDs, we can sense them closing
        # values are ProcessReader/ProcessWriters

        helpers = {}
        # keys are childFDs
        # values are parentFDs

        if childFDs is None:
            childFDs = {0: "w", # we write to the child's stdin
                        1: "r", # we read from their stdout
                        2: "r", # and we read from their stderr
                        }

        debug = self.debug
        if debug: print "childFDs", childFDs

        # fdmap.keys() are filenos of pipes that are used by the child.
        fdmap = {} # maps childFD to parentFD
        for childFD, target in childFDs.items():
            if debug: print "[%d]" % childFD, target
            if target == "r":
                # we need a pipe that the parent can read from
                readFD, writeFD = os.pipe()
                if debug: print "readFD=%d, writeFD%d" % (readFD, writeFD)
                fdmap[childFD] = writeFD     # child writes to this
                helpers[childFD] = readFD    # parent reads from this
            elif target == "w":
                # we need a pipe that the parent can write to
                readFD, writeFD = os.pipe()
                if debug: print "readFD=%d, writeFD=%d" % (readFD, writeFD)
                fdmap[childFD] = readFD      # child reads from this
                helpers[childFD] = writeFD   # parent writes to this
            else:
                assert type(target) == int, '%r should be an int' % (target,)
                fdmap[childFD] = target      # parent ignores this
        if debug: print "fdmap", fdmap
        if debug: print "helpers", helpers
        # the child only cares about fdmap.values()

        self.pid = os.fork()
        if self.pid == 0: # pid is 0 in the child process

            # do not put *ANY* code outside the try block. The child process
            # must either exec or _exit. If it gets outside this block (due
            # to an exception that is not handled here, but which might be
            # handled higher up), there will be two copies of the parent
            # running in parallel, doing all kinds of damage.

            # After each change to this code, review it to make sure there
            # are no exit paths.

            try:
                # stop debugging, if I am!  I don't care anymore!
                sys.settrace(None)
                # close all parent-side pipes
                self._setupChild(fdmap)
                # Make a session/process group leader if requested
                if sessionLeader: self._setupSession()
                self._execChild(path, settingUID, uid, gid,
                                command, args, environment)
            except:
                # If there are errors, bail and try to write something
                # descriptive to stderr.
                # XXX: The parent's stderr isn't necessarily fd 2 anymore, or
                #      even still available
                # XXXX: however even libc assumes write(2,err) is a useful
                #       thing to attempt
                try:
                    stderr = os.fdopen(2,'w')
                    stderr.write("Upon execvpe %s %s in environment %s\n:" %
                                 (command, str(args),
                                  "id %s" % id(environment)))
                    traceback.print_exc(file=stderr)
                    stderr.flush()
                    for fd in range(3):
                        os.close(fd)
                except:
                    pass # make *sure* the child terminates
            # Did you read the comment about not adding code here?
            os._exit(1)

        # we are the parent

        if settingUID:
            os.setregid(currgid, curegid)
            os.setreuid(curruid, cureuid)
        self.status = -1 # this records the exit status of the child

        if timeout:
            self.timeoutCall = reactor.callLater(timeout, self._processTimeout)

        self.proto = proto
        
        # arrange for the parent-side pipes to be read and written
        for childFD, parentFD in helpers.items():
            os.close(fdmap[childFD])

            if childFDs[childFD] == "r":
                reader = process.ProcessReader(reactor, self, childFD, parentFD)
                self.pipes[childFD] = reader

            if childFDs[childFD] == "w":
                writer = process.ProcessWriter(reactor, self, childFD, parentFD, forceReadHack=True)
                self.pipes[childFD] = writer

        try:
            # the 'transport' is used for some compatibility methods
            if self.proto is not None:
                self.proto.makeConnection(self)
        except:
            log.err()
        process.registerReapProcessHandler(self.pid, self)

    def processEnded(self, status):
        if self.timeoutCall:
            try: self.timeoutCall.cancel()
            except: pass

        #self.status = status
        #self.lostProcess = True
        
        pgid = -self.pid
        
        try:
            process.Process.processEnded(self, status)
        finally:
            
            # The process group leader may have terminated, but child process in
            # the group may still be alive. Mass slaughter.
            try:
                self.signalProcessGroup(signal.SIGKILL, pgid)
            except OSError, e:
                if e.errno == errno.EPERM:
                    self.proto.leftoverProcesses(False)
                elif e.errno != errno.ESRCH:
                    print "pgid:", pgid, "e:", e
                    raise
            else:
                self.proto.leftoverProcesses(True)  
        #finally:
        #    self.pid = None
        #    self.maybeCallProcessEnded()

    def _setupSession(self):
        os.setsid()
    
    def _processTimeout(self):
        """
        Called when the timeout expires.
        """
        
        # Kill the process group
        if not self.lostProcess:
            self.signalProcessGroup(signal.SIGKILL)
    
    def signalProcessGroup(self, signal, pgid=None):
        os.kill(pgid or -self.pid, signal)

class RunCommandMonitoringProtocol(monitor.MonitoringProtocol):
    """
    Monitor that checks server uptime by repeatedly fetching a certain URL
    """
    
    __name__ = 'RunCommand'
    
    INTV_CHECK = 60
    
    TIMEOUT_RUN = 20
    
    def __init__(self, coordinator, server, configuration={}):
        """Constructor"""   
            
        # Call ancestor constructor
        super(RunCommandMonitoringProtocol, self).__init__(coordinator, server, configuration)

        locals = {  'server':   server
        }

        self.intvCheck = self._getConfigInt('interval', self.INTV_CHECK)
        self.timeout = self._getConfigInt('timeout', self.TIMEOUT_RUN)
        self.command = self._getConfigString('command')
        self.arguments = self._getConfigStringList('arguments', locals=locals)
        self.logOutput = self._getConfigBool('log-output', True)

        self.checkCall = None
        self.runningProcess = None

        # Install cleanup handler
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        
    def run(self):
        """Start the monitoring"""
        
        super(RunCommandMonitoringProtocol, self).run()
        
        if not self.checkCall or not self.checkCall.active():
            self.checkCall = reactor.callLater(self.intvCheck, self.runCommand)

    def stop(self):
        """Stop all running and/or upcoming checks"""
        
        super(RunCommandMonitoringProtocol, self).stop()

        if self.checkCall and self.checkCall.active():
            self.checkCall.cancel()

        # Try to kill any running check
        if self.runningProcess is not None:
            try: self.runningProcess.signalProcess(signal.SIGKILL)
            except error.ProcessExitedAlready: pass
    
    def runCommand(self):
        """Periodically called method that does a single uptime check."""

        self.runningProcess = self._spawnProcess(self, self.command, [self.command] + self.arguments,
                                                 sessionLeader=True, timeout=(self.timeout or None))
    
    def makeConnection(self, process):
        pass

    def childDataReceived(self, childFD, data):        
        if not self.logOutput: return
        
        # Escape control chars
        map = {'\n': r'\n',
               '\r': r'\r',
               '\t': r'\t'}
        for char, subst in map.iteritems():
            data = data.replace(char, subst)
        
        self.report("Cmd stdout: " + data)

    def childConnectionLost(self, childFD):
        pass
    
    def processEnded(self, reason):
        """
        Called when the process has ended
        """
        
        if reason.check(error.ProcessDone):
            self._resultUp()
        elif reason.check(error.ProcessTerminated):
            self._resultDown(reason.getErrorMessage())
        
        # Schedule the next check
        if self.active:
            self.checkCall = reactor.callLater(self.intvCheck, self.runCommand)

        reason.trap(error.ProcessDone, error.ProcessTerminated)

    def leftoverProcesses(self, allKilled):
        """
        Called when the child terminated cleanly, but left some of
        its child processes behind
        """
        
        if allKilled:
            msg = "WARNING: Command %s %s left child processes behind, which have been killed!"
        else:
            msg = "WARNING: Command %s %s left child processes behind, and not all could be killed!"
        self.report(msg % (self.command, str(self.arguments)))

    def _spawnProcess(self, processProtocol, executable, args=(),
                     env={}, path=None,
                     uid=None, gid=None, childFDs=None,
                     sessionLeader=False, timeout=None):
        """
        Replacement for posixbase.PosixReactorBase.spawnProcess with added
        process group / session and timeout support, and support for 
        non-POSIX platforms and PTYs removed.
        """       
        
        args, env = reactor._checkProcessArgs(args, env)
        return ProcessGroupProcess(reactor, executable, args, env, path,
                               processProtocol, uid, gid, childFDs,
                               sessionLeader, timeout)
