#!/usr/bin/python

"""
PyBal
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

LVS Squid balancer/monitor for managing the Wikimedia Squid servers using LVS

$Id$
"""

import os, sys, signal

import ipvs, monitor

# TODO: make more dynamic
from monitors import *

class Server:
    """
    Class that maintains configuration and state of a single (real)server
    """
    
    # Defaults
    DEF_STATE = True
    DEF_WEIGHT = 10
    
    # Set of attributes allowed to be overridden in a server list
    allowedConfigKeys = [ ('host', str), ('weight', int), ('enabled', bool) ]
    
    def __init__(self, host):
        """Constructor"""        
        
        self.host = host
        self.port = 80
        
        self.monitors = []
        
        self.weight = self.DEF_WEIGHT
        self.up = self.DEF_STATE
        self.pooled = self.up
        self.enabled = self.up
        
    def addMonitor(self, monitor):
        """Adds a monitor instance to the list"""
        
        if monitor not in self.monitors:
            self.monitors.append(monitor)
    
    def removeMonitor(self, monitor):
        """Stops and removes a monitor instance from the list"""
        
        monitor.stop()
        self.monitors.remove(monitor)    # May raise exception if not exists
    
    def removeMonitors(self):
        """Removes all monitors"""
        
        for monitor in self.monitors:
            self.removeMonitor(monitor)
    
    def merge(self, server):
        """Merges in configuration attributes of another instance"""
        
        for key, value in server.__dict__.iteritems():
            if (key, type(value)) in self.allowedConfigKeys:
                self.__dict__[key] = value
    
    def buildServer(cls, configuration):
        """
        Factory method which builds a Server instance from a
        dictionary of (allowed) configuration attributes
        """

        for key, value in configuration.iteritems():
            if (key, type(value)) not in cls.allowedConfigKeys:
                del configuration[key]
        
        server = cls(configuration['host'])        # create a new instance...
        server.__dict__.update(configuration)      # ...and override attributes
        
        return server
    buildServer = classmethod(buildServer)

class Coordinator:
    """
    Class that coordinates the configuration, state and status reports
    for a single LVS instance
    """
    
    serverConfigURL = 'file:///etc/pybal/squids'
    
    intvLoadServers = 60
    
    def __init__(self, lvsservice, configURL):
        """Constructor"""
        
        self.lvsservice = lvsservice
        self.servers = {}
        self.pooledDownServers = []
        self.configHash = None
        self.serverConfigURL = configURL
        
        # Start a periodic server list update task
        from twisted.internet import task
        task.LoopingCall(self.loadServers).start(self.intvLoadServers)
    
    def assignServers(self, servers):
        """
        Takes a new set of servers (as a host->Server dict) and
        hands them over to LVSService
        """
                
        self.servers = servers
        
        # Hand over enabled servers to LVSService
        self.lvsservice.assignServers(
            dict([(server.host, server) for server in servers.itervalues() if server.enabled]))
    
    def createMonitoringInstances(self, servers=None):
        """Creates and runs monitoring instances for a list of Servers"""        
        
        # Use self.servers by default
        if servers is None:
            servers = self.servers.itervalues()
        
        for server in servers:
            if not server.enabled: continue
            
            try:
                monitorlist = eval(self.lvsservice.configuration['monitors'])
            except KeyError:
                print "LVS service", self.lvsservice.name, "does not have a 'monitors' configuration option set."

            if type(monitorlist) != list:
                print "option 'monitors' in LVS service section", self.lvsservice.name, \
                    "is not a Python list."
            else:                
                for monitorname in monitorlist:
                    try:
                        # FIXME: this is a hack?
                        monitormodule = getattr(sys.modules['monitors'], monitorname.lower())
                        monitorclass = getattr(monitormodule , monitorname + 'MonitoringProtocol' )
                        server.addMonitor(monitorclass(self, server, self.lvsservice.configuration))
                    except AttributeError:
                        print "Monitor", monitorname, "does not exist."
                
            # Set initial status
            #server.up = self.calcStatus(server)
            
            # Run all instances
            for monitor in server.monitors:
                monitor.run()

    def resultDown(self, monitor, reason=None):
        """
        Accepts a 'down' notification status result from a single monitoring instance
        and acts accordingly.
        """
        
        server = monitor.server
        
        print 'Monitoring instance', monitor.name(), 'reports server', server.host, 'down:', (reason or '(reason unknown)')
        
        if server.up:
            server.up = False
            self.depool(server)

    def resultUp(self, monitor):
        """
        Accepts a 'up' notification status result from a single monitoring instance
        and acts accordingly.
        """
        
        server = monitor.server
    
        if not server.up and self.calcStatus(server):
            server.up = True
            self.repool(server)
            
            print 'Server', server.host, 'is up'
    
    def calcStatus(self, server):
        """AND quantification of monitor.up over all monitoring instances of a single Server"""
        
        # Global status is up iff all monitors report up
        return reduce(lambda b,monitor: b and monitor.up, server.monitors, server.monitors != [])            

    def depool(self, server):
        """Depools a single Server, if possible"""
        
        if not server.pooled: return
        
        if self.canDepool(server):
            self.lvsservice.removeServer(server)
            try: self.pooledDownServers.remove(server)
            except ValueError: pass
        else:
            if server not in self.pooledDownServers:
                self.pooledDownServers.append(server)
            print 'Could not depool server', server.host, 'because of too many down!'
    
    def repool(self, server):
        """
        Repools a single server. Also depools previously downed Servers that could
        not be depooled then because of too many hosts down.
        """
        
        if not server.pooled and server.enabled:
            self.lvsservice.addServer(server)
        
        # If it had been pooled in down state before, remove it from the list
        try: self.pooledDownServers.remove(server)
        except ValueError: pass

        # See if we can depool any servers that could not be depooled before
        for server in self.pooledDownServers:
            if self.canDepool(server):
                self.depool(server)
            else:    # then we can't depool any further servers either...
                break

    def canDepool(self, server):
        """Returns a boolean denoting whether another server can be depooled"""
        
        # Construct a list of servers that have status 'down'
        downServers = [server for server in self.servers.itervalues() if not server.up]
        
        # Only allow depooling if less than half of the total amount of servers are down
        return len(downServers) <= len(self.servers) / 2
    
    def loadServers(self, configURL=None):
        """Periodic task to load a new server list/configuration file from a specified URL."""
        
        configURL = configURL or self.serverConfigURL
        
        if configURL.startswith('http://'):
            # Retrieve file over HTTP
            from twisted.web import client
            client.getPage(configURL).addCallback(self._configReceived)
        elif configURL.startswith('file://'):
            # Read the text file
            try:
                self._configReceived(open(configURL[7:], 'r').read())
            except IOError, e:
                print e
        else:
            raise ValueError, "Invalid configuration URL"
    
    def _configReceived(self, configuration):
        """
        Compares the MD5 hash of the new configuration vs. the old one,
        and calls _parseConfig if it's different.
        """
        
        import md5
        newHash = md5.new(configuration)
        if not self.configHash or self.configHash.digest() != newHash.digest():
            print 'New configuration received'
            
            self.configHash = newHash        
            self._parseConfig(configuration.splitlines())
    
    def _parseConfig(self, lines):
        """Parses the server list and changes the state accordingly."""
        
        delServers = self.servers.copy()    # Shallow copy
        setupMonitoring = []
             
        for line in lines:
            line = line.rstrip('\n').strip()
            if line.startswith('#') or line == '': continue
            
            serverdict = eval(line)
            if type(serverdict) == dict and 'host' in serverdict:
                host = serverdict['host']
                if host in self.servers:
                    # Existing server. merge
                    server = delServers.pop(host)
                    newServer = Server.buildServer(serverdict)

                    print "Merging server %s, weight %d" % ( host, newServer.weight )

                    # FIXME: Doesn't "enabled" mean "monitored, but not pooled"?
                    if not newServer.enabled and server.enabled:
                        server.removeMonitors()
                    elif newServer.enabled and not server.enabled:
                        setupMonitoring.append(newServer)

                    server.merge(newServer)
                else:
                    # New server
                    server = Server.buildServer(serverdict)
                    self.servers[host] = server
                    
                    print "New server %s, weight %d" % ( host, server.weight )
                    
                    setupMonitoring.append(server)
        
        self.createMonitoringInstances(setupMonitoring)
        
        # Remove old servers
        for host, server in delServers.iteritems():
            server.enabled = False
            server.removeMonitors()
            del self.servers[host]
        
        self.assignServers(self.servers)    # FIXME        

def parseCommandLine(configuration):
    """
    Parses the command line arguments, and sets configuration options
    in dictionary configuration.
    """
    
    import sys, getopt

    options = 'hnd'
    long_options = [ 'help', 'dryrun', 'debug' ]
    
    for o, a in getopt.gnu_getopt(sys.argv, options, long_options)[0]:
        if o in ('-h', '--help'):
            printHelp()
            sys.exit(0)
        elif o in ('-n', '--dryrun'):
            configuration['dryrun'] = True
        elif o in ('-d', '--debug'):
            configuration['debug'] = True

def printHelp():
    """Prints a help screen"""
    
    print "Usage:"
    print "\tpybal [ options ]"
    print "\t\t-h\t--help\t\tThis help message"
    print "\t\t-n\t--dryrun\tDry Run mode, do not actually update"
    print "\t\t-d\t--debug\tDebug mode, do not daemonize, log to stdout"
    print "\t\t\t\t\tLVS configuration/state, but print commands"

def createDaemon():
    """
    Detach a process from the controlling terminal and run it in the
    background as a daemon.
    """

    try:
        # Fork a child process so the parent can exit.  This will return control
        # to the command line or shell.  This is required so that the new process
        # is guaranteed not to be a process group leader.  We have this guarantee
        # because the process GID of the parent is inherited by the child, but
        # the child gets a new PID, making it impossible for its PID to equal its
        # PGID.
        pid = os.fork()
    except OSError, e:
        return( ( e.errno, e.strerror ) )     # ERROR (return a tuple)

    if ( pid == 0 ):       # The first child.
        # Next we call os.setsid() to become the session leader of this new
        # session.  The process also becomes the process group leader of the
        # new process group.  Since a controlling terminal is associated with a
        # session, and this new session has not yet acquired a controlling
        # terminal our process now has no controlling terminal.  This shouldn't
        # fail, since we're guaranteed that the child is not a process group
        # leader.
        os.setsid()

        # When the first child terminates, all processes in the second child
        # are sent a SIGHUP, so it's ignored.
        signal.signal( signal.SIGHUP, signal.SIG_IGN )

        try:
            # Fork a second child to prevent zombies.  Since the first child is
            # a session leader without a controlling terminal, it's possible for
            # it to acquire one by opening a terminal in the future.  This second
            # fork guarantees that the child is no longer a session leader, thus
            # preventing the daemon from ever acquiring a controlling terminal.
            pid = os.fork()        # Fork a second child.
        except OSError, e:
            return( ( e.errno, e.strerror ) )  # ERROR (return a tuple)

        if ( pid == 0 ):      # The second child.
            # Ensure that the daemon doesn't keep any directory in use.  Failure
            # to do this could make a filesystem unmountable.
            os.chdir( "/" )
            # Give the child complete control over permissions.
            os.umask( 0 )
        else:
            os._exit( 0 )      # Exit parent (the first child) of the second child.
    else:
        os._exit( 0 )         # Exit parent of the first child.

    # Close all open files.  Try the system configuration variable, SC_OPEN_MAX,
    # for the maximum number of open files to close.  If it doesn't exist, use
    # the default value (configurable).
    try:
        maxfd = os.sysconf( "SC_OPEN_MAX" )
    except ( AttributeError, ValueError ):
        maxfd = 256       # default maximum

    for fd in range( 0, maxfd ):
        try:
            os.close( fd )
        except OSError:   # ERROR (ignore)
            pass

    # Redirect the standard file descriptors to /dev/null.
    os.open( "/dev/null", os.O_RDONLY )    # standard input (0)
    os.open( "/dev/null", os.O_RDWR )       # standard output (1)
    os.open( "/dev/null", os.O_RDWR )       # standard error (2)

    return( 0 )

def writePID(): 
    """
    Writes the current processes's PID into /var/run/pybal.pid
    """
    
    try:
        file('/var/run/pybal.pid', 'w').write(str(os.getpid()) + '\n')
    except:
        raise

def terminate():
    """
    Cleans up on exit
    """
        
    # Remove any PID file
    print "Removing PID file /var/run/pybal.pid"
    try:
        os.unlink('/var/run/pybal.pid')
    except OSError:
        pass
    
    print "Exiting..."

def sighandler(signum, frame):
    """
    Signal handler
    """
    
    if signum == signal.SIGTERM:
        terminate()

def installSignalHandlers():
    """
    Installs Unix signal handlers, e.g. to run terminate() on TERM
    """
    
    signal.signal(signal.SIGTERM, sighandler)

def main():
    from twisted.internet import reactor
    from ConfigParser import SafeConfigParser
      
    # Read the configuration file
    configFile = '/etc/pybal/pybal.conf'
    
    config = SafeConfigParser()
    config.read(configFile)
    
    services, cliconfig = {}, {}
    
    # Parse the command line
    parseCommandLine(cliconfig)

    try:
        if not cliconfig.get('debug', False):
            # Become a daemon
            createDaemon()
            
            # Write PID file
            writePID()
            
            # Open a logfile
            from util import LogFile
            try:
                logfile = '/var/log/pybal.log'
                sys.stdout = LogFile(logfile)
            except:
                print "Unable to open logfile %s, using stdout" % logfile  

        # Install signal handlers
        installSignalHandlers

        for section in config.sections():
            cfgtuple = (
                config.get(section, 'protocol'),
                config.get(section, 'ip'),
                config.getint(section, 'port'),
                config.get(section, 'scheduler'))
                
            # Read the custom configuration options of the LVS section
            configdict = dict(config.items(section))
            
            # Override with command line options
            configdict.update(cliconfig)
                    
            services[section] = ipvs.LVSService(section, cfgtuple, configuration=configdict)
            crd = Coordinator(services[section],
                configURL=config.get(section, 'config'))
            print "Created LVS service '%s'" % section
        
        reactor.run()
    finally:
        terminate()

if __name__ == '__main__':
    main()