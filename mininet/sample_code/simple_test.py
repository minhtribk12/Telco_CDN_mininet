#!/usr/bin/python                                                                            
                                                                                             
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.node import RemoteController
from mininet.link import Link, TCLink
from mininet.cli import CLI 
from time import sleep
import os, os.path

class RingTopo(Topo):
    def __init__(self, **opts):
        # Initialize topology and default options
        Topo.__init__(self, **opts)

        # Add switch
        switch0 = self.addSwitch('s0', protocols='OpenFlow13')
        switch1 = self.addSwitch('s1', protocols='OpenFlow13')
        switch2 = self.addSwitch('s2', protocols='OpenFlow13')
        switch3 = self.addSwitch('s3', protocols='OpenFlow13')
        switch100 = self.addSwitch('s100', protocols='OpenFlow13')
        
        # Add host
        host0 = self.addHost('h0', mac='00:00:00:00:00:01', ip='10.0.0.1/24' )
        host1 = self.addHost('h1', mac='00:00:00:00:00:02', ip='10.0.0.2/24' )
        host2 = self.addHost('h2', mac='00:00:00:00:00:03', ip='10.0.0.3/24' )
        host3 = self.addHost('h3', mac='00:00:00:00:00:04', ip='10.0.0.4/24' )
        host100 = self.addHost('h100', mac='00:00:00:00:00:64', ip='10.0.0.101/24' )

        # Add link
        self.addLink(switch0, switch1)
        self.addLink(switch1, switch2)
        self.addLink(switch2, switch3)
        self.addLink(switch3, switch0)
        self.addLink(switch100, switch2)
        self.addLink(host0, switch0)
        self.addLink(host1, switch1)
        self.addLink(host2, switch2)
        self.addLink(host3, switch3)
        self.addLink(host100, switch100)
        
        # Python's range(N) generates 0..N-1
        # for h in range(n):
        #     host = self.addHost('h%s' % (h + 1))
        #     self.addLink(host, switch)

def simpleTest():
    topo = RingTopo()
    controller_ip = '127.0.0.1'
    net = Mininet(topo=topo, controller=lambda a: RemoteController(a, ip=controller_ip, port=6633), link=TCLink)
    net.start()
    print "Dumping host connections"
    dumpNodeConnections(net.hosts)
    print "Testing network connectivity"
    sleep(5)
    net.pingAll()
    h0 = net.getNodeByName("h0")
    h1 = net.getNodeByName("h1")
    h2 = net.getNodeByName("h2")
    h3 = net.getNodeByName("h3")
    h100 = net.getNodeByName("h100")
    h0.cmd('python mininet/source/cache_algorithm/cache_color.py -i 0 &')
    h1.cmd('python mininet/source/cache_algorithm/cache_color.py -i 1 &')
    h2.cmd('python mininet/source/cache_algorithm/cache_color.py -i 2 &')
    h3.cmd('python mininet/source/cache_algorithm/cache_color.py -i 3 &')
    print h100.cmd('python mininet/source/cache_algorithm/cache_color.py -i 100')
    DIR = '~/workspace/telco_cdn_mininet/mininet/source/cache_algorithm/result'
    if not os.path.exists(DIR):
        os.makedirs(DIR)
    while True:
        if (len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))]) >= 4):
            break
        sleep(1)
    net.stop()

if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    simpleTest()