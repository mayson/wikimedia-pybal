"""
ipvsadm.py
Copyright (C) 2006 by Mark Bergsma <mark@nedworks.org>

LVS state/configuration classes for PyBal

$Id$
"""

import os

class IPVSManager:
    """
    Class that provides a mapping from abstract LVS commands / state changes
    to ipvsadm command invocations.
    """
    
    ipvsPath = '/sbin/ipvsadm'

    DryRun = True

    def modifyState(cls, cmdList):
        """
        Changes the state using a supplied list of commands (by invoking ipvsadm)
        """
        
        print cmdList
        if cls.DryRun: return
        
        command = [cls.ipvsPath, '-R']
        stdin = os.popen(" ".join(command), 'w')
        for line in cmdList:
            stdin.write(line + '\n')
        stdin.close()
    modifyState = classmethod(modifyState)
    
    def subCommandService(service):
        """
        Returns a partial command / parameter list as a single string,
        that describes the supplied LVS service.
        
        Arguments:
            service:    tuple(protocol, address, port, ...)
        """
        
        return {'tcp': '-t',
                'udp': '-u'}[service[0]] + ' %s:%d' % service[1:3]
    subCommandService = staticmethod(subCommandService)
    
    def commandClearServiceTable():
        """Returns an ipvsadm command to clear the current service table."""
        
        return '-C'
    commandClearServiceTable = staticmethod(commandClearServiceTable)
    
    def commandRemoveService(cls, service):
        """Returns an ipvsadm command to remove a single service."""
        
        return '-D ' + cls.subCommandService(service)
    commandRemoveService = classmethod(commandRemoveService)
    
    def commandAddService(cls, service):
        """
        Returns an ipvsadm command to add a specified service.
        
        Arguments:
            service:    tuple(protocol, address, port, ...)
        """
        
        cmd = '-A ' + cls.subCommandService(service)
        
        # Include scheduler if specified
        if len(service) > 3:
            cmd += ' -s ' + service[3]
            
        return cmd
    commandAddService = classmethod(commandAddService)
    
    def commandRemoveServer(cls, service, server):
        """
        Returns an ipvsadm command to remove a server from a service.
        
        Arguments:
            service:   tuple(protocol, address, port, ...)
            server:    Server
        """
                
        return '-d ' + cls.subCommandService(service) + ' -r ' + server.host
    commandRemoveServer = classmethod(commandRemoveServer)
    
    def commandAddServer(cls, service, server):
        """
        Returns an ipvsadm command to add a server to a service.
        
        Arguments:
            service:   tuple(protocol, address, port, ...)
            server:    Server
        """        
        
        cmd = '-a ' + cls.subCommandService(service) + ' -r ' + server.host
        
        # Include weight if specified
        if server.weight:
            cmd += ' -w %d' % server.weight

        return cmd
    commandAddServer = classmethod(commandAddServer)
    
    def commandEditServer(cls, service, server):
        """
        Returns an ipvsadm command to edit the parameters of a server.
        
        Arguments:
            service:   tuple(protocol, address, port, ...)
            server:    Server
        """        
        
        cmd = '-e ' + cls.subCommandService(service) + ' -r ' + server.host
        
        # Include weight if specified
        if server.weight:
            cmd += ' -w %d' % server.weight
        
        return cmd
    commandEditServer = classmethod(commandEditServer)

class LVSService:
    """
    Class that maintains the state of a single LVS service instance
    """
    
    ipvsManager = IPVSManager
    
    SVC_PROTOS = ('tcp', 'udp')
    SVC_SCHEDULERS = ('rr', 'wrr', 'lc', 'wlc', 'lblc', 'lblcr', 'dh', 'sh', 'sed', 'nq')

    def __init__(self, (protocol, ip, port, scheduler), servers={}):
        """Constructor"""
        
        self.servers = servers
        
        if (protocol not in self.SVC_PROTOS
            or scheduler not in self.SVC_SCHEDULERS):
            raise ValueError, "Invalid protocol or scheduler"
        
        self.protocol = protocol
        self.ip = ip
        self.port = port
        self.scheduler = scheduler
        
        self.createService()
    
    def service(self):
        """
        Returns a tuple (protocol, ip, port, scheduler) that describes
        this LVS instance
        """
        
        return (self.protocol, self.ip, self.port, self.scheduler)
    
    def createService(self):
        """
        Initializes this LVS instance in LVS
        """
        
        # Remove a previous service and add the new one
        cmdList = [self.ipvsManager.commandRemoveService(self.service()),
                   self.ipvsManager.commandAddService(self.service())]
        
        # Add realservers
        for server in self.servers.itervalues():
            cmdList.append(self.ipvsManager.commandAddServer(self.service(), server))
        
        self.ipvsManager.modifyState(cmdList)

    def assignServers(self, newServers):
        """
        Takes a (new) set of servers (as a host->Server dictionary) and updates
        the LVS state accordingly.
        """
                
        # Compute set of servers to delete and edit
        removeServers, editServers = [], []
        for hostname, server in self.servers.iteritems():
            if hostname not in newServers:
                removeServers.append(server)
            else:
                editServers.append(server)
        
        # Compute set of servers to add
        addServers = []
        for hostname, server in newServers.iteritems():
            if hostname not in self.servers:
                addServers.append(server)
        
        self.servers = dict(newServers) # shallow copy
        cmdList = []
        
        # Add new servers first
        for server in addServers:
            cmdList.append(self.ipvsManager.commandAddServer(self.service(), server))
            server.pooled = True
        
        # Edit existing servers
        for server in editServers:
            cmdList.append(self.ipvsManager.commandEditServer(self.service(), server))

        # Remove servers
        for server in removeServers:
            cmdList.append(self.ipvsManager.commandRemoveServer(self.service(), server))
            server.pooled = False

        self.ipvsManager.modifyState(cmdList)
    
    def addServer(self, server):
        """Adds (pools) a single Server to the LVS state"""
        
        if server.host not in self.servers:
            cmdList = [self.ipvsManager.commandAddServer(self.service(), server)]
        else:
            cmdList = [self.ipvsManager.commandEditServer(self.service(), server)]
            
        self.servers[server.host] = server
        
        self.ipvsManager.modifyState(cmdList)
        server.pooled = True
    
    def removeServer(self, server):
        """Removes (depools) a single Server from the LVS state"""
        
        cmdList = [self.ipvsManager.commandRemoveServer(self.service(), server)]
        
        del self.servers[server.host]    # May raise KeyError

        server.pooled = False        
        self.ipvsManager.modifyState(cmdList)