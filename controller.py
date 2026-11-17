#!/usr/bin/python

from mininet.net import Mininet
from mininet.node import OVSKernelSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink

def create_job_queue_topology():
    net = Mininet(switch=OVSKernelSwitch, controller=None, link=TCLink)

    info('*** Adding Switches (Standalone Learning Mode)\n')
    s1 = net.addSwitch('s1', failMode='standalone')
    s2 = net.addSwitch('s2', failMode='standalone')

    info('*** Adding Hosts (Clients, Broker, Workers)\n')
    h_client = net.addHost('h1', ip='10.0.0.1', mac='00:00:00:00:00:01')
    h_broker = net.addHost('h2', ip='10.0.0.2', mac='00:00:00:00:00:02')
    h_worker1 = net.addHost('h3', ip='10.0.0.3', mac='00:00:00:00:00:03')
    h_worker2 = net.addHost('h4', ip='10.0.0.4', mac='00:00:00:00:00:04')

    info('*** Creating Links\n')
    # Connect hosts to switches
    net.addLink(h_client, s1, bw=100, delay='2ms')
    net.addLink(h_broker, s1, bw=100, delay='2ms')
    net.addLink(h_worker1, s2, bw=100, delay='2ms')
    net.addLink(h_worker2, s2, bw=100, delay='2ms')

    # Connect core switches with a reliable redundant pair
    net.addLink(s1, s2, bw=50, delay='5ms')

    info('*** Starting Network\n')
    net.start()

    info('*** Running CLI (Type exit or Ctrl+D to quit)\n')
    CLI(net)

    info('*** Stopping Network\n')
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    create_job_queue_topology()
