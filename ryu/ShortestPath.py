import os
import pickle
import time

import networkx as nx
from ryu.app.ofctl.api import get_datapath
from ryu.app.wsgi import ControllerBase
from ryu.base import app_manager
from ryu.controller import dpset
from ryu.controller import mac_to_port
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib.mac import haddr_to_bin
from ryu.lib.packet import arp
from ryu.lib.packet import ethernet
from ryu.lib.packet import icmp
from ryu.lib.packet import ipv4
from ryu.lib.packet import lldp
from ryu.lib.packet import packet
from ryu.lib.packet import packet_base
from ryu.lib.packet import tcp
from ryu.lib.packet import udp
from ryu.lib.packet import vlan
from ryu.ofproto import ether
from ryu.ofproto import ofproto_v1_3
from ryu.topology.api import get_switch, get_link
from ryu.lib.packet import ether_types

ETHERNET = ethernet.ethernet.__name__
VLAN = vlan.vlan.__name__
IPV4 = ipv4.ipv4.__name__
ARP = arp.arp.__name__
ICMP = icmp.icmp.__name__
TCP = tcp.tcp.__name__
UDP = udp.udp.__name__
LLDP = lldp.lldp.__name__
UINT16_MAX = 0xffff
UINT32_MAX = 0xffffffff
UINT64_MAX = 0xffffffffffffffff



class ProjectController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ProjectController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_api_app = self
        self.net = nx.DiGraph()
        self.topo_check_time = time.time()
        self.router = {}

        self.route_table = []

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

    @staticmethod
    def set_sp_routing_flow(dp, cookie, priority, outport,
                            src_mac, dst_mac, idle_timeout=0, dec_ttl=False):
        ofp = dp.ofproto
        ofp_parser = dp.ofproto_parser

        actions = []

        if outport is not None:
            actions.append(ofp_parser.OFPActionOutput(outport))

        match = ofp_parser.OFPMatch(
                                    eth_dst=dst_mac,
                                    eth_src=src_mac)

        inst = [ofp_parser.OFPInstructionActions(ofp.OFPIT_APPLY_ACTIONS,
                                                 actions)]

        mod = ofp_parser.OFPFlowMod(
            datapath=dp, match=match, cookie=cookie,
            command=ofp.OFPFC_ADD, idle_timeout=idle_timeout, hard_timeout=0,
            priority=priority,
            flags=ofp.OFPFF_SEND_FLOW_REM, instructions=inst)

        dp.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(data=msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if (time.time() - self.topo_check_time) > 2:
            self.get_topology_data()
            self.topo_check_time = time.time()

        if eth.ethertype == ether_types.ETH_TYPE_LLDP or eth.ethertype == ether_types.ETH_TYPE_IPV6:
            return

        if eth.ethertype == ether_types.ETH_TYPE_IP or eth.ethertype == ether_types.ETH_TYPE_ARP:

            dst = eth.dst
            src = eth.src
            dpid = datapath.id

            key_1 = src + '-' + dst
            key_2 = dst + '-' + src
            know_route = False
            out_ports = {}
            out_ports_2 = {}
            next_out_port = ofproto.OFPP_FLOOD
            if src not in self.net:
                self.net.add_node(src)
                self.net.add_edge(dpid, src)
                self.net.edges[dpid, src].update({'port': in_port})
                self.net.add_edge(src, dpid)

            if dst in self.net:
                path = nx.shortest_path(self.net, src, dst)
                print('[%d] src: %s | dst: %s | Path: %s' % (dpid, src, dst, str(path)))

                if dpid not in path:
                    return

                for node in range(1, len(path) - 1):
                    current_dpid = path[node]
                    next = path[node + 1]
                    out_ports[current_dpid] = self.net[current_dpid][next]['port']

                path_2 = nx.shortest_path(self.net, dst, src)
                print('[%d]src: %s | dst: %s | Path: %s' % (dpid, dst, src, str(path_2)))

                for node in range(1, len(path_2) - 1):
                    current_dpid = path_2[node]
                    next = path_2[node + 1]
                    out_ports_2[current_dpid] = self.net[current_dpid][next]['port']

                know_route = True

                next = path[path.index(dpid) + 1]
                next_out_port = self.net[dpid][next]['port']

            if know_route:

                dpid_keys = out_ports.keys()
                for dpid_key in dpid_keys:
                    out_port = out_ports[dpid_key]

                    if dpid_key not in self.router.keys():
                        continue

                    datapath_dpid = self.router[dpid_key]

                    route_entry = str(dpid_key) + src + dst + str(out_port)
                    if route_entry in self.route_table:
                        continue
                    else:
                        self.route_table.append(route_entry)

                    # install a flow to avoid packet_in next time
                    print '[%d] Add flow to %s at sw %d - outport %d' % (dpid, dst, dpid_key, out_port)
                    self.set_sp_routing_flow(dp=datapath_dpid,
                                             cookie=0,
                                             priority=100,
                                             outport=out_port,
                                             dst_mac=dst,
                                             src_mac=src,
                                             dec_ttl=True)

                dpid_keys_2 = out_ports_2.keys()

                for dpid_key in dpid_keys_2:
                    out_port = out_ports_2[dpid_key]

                    if dpid_key not in self.router.keys():
                        continue

                    datapath_dpid = self.router[dpid_key]

                    route_entry = str(dpid_key) + dst + src + str(out_port)
                    if route_entry in self.route_table:
                        continue
                    else:
                        self.route_table.append(route_entry)

                    # install a flow to avoid packet_in next time
                    print '[%d] Add flow inverse to %s at sw %d - outport %d' % (dpid, src, dpid_key, out_port)
                    self.set_sp_routing_flow(dp=datapath_dpid,
                                             cookie=0,
                                             priority=100,
                                             outport=out_port,
                                             dst_mac=src,
                                             src_mac=dst,
                                             dec_ttl=True)

            actions = [parser.OFPActionOutput(next_out_port)]

            data = None
            if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                data = msg.data

            out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                      in_port=in_port, actions=actions, data=data)
            datapath.send_msg(out)


    def get_topology_data(self):
        switch_list = get_switch(self.topology_api_app, None)

        switches = [switch.dp.id for switch in switch_list]
        self.net.add_nodes_from(switches)

        links_list = get_link(self.topology_api_app, None)

        links = [(link.src.dpid, link.dst.dpid, {'port': link.src.port_no}) for link in links_list]

        self.net.add_edges_from(links)
        links = [(link.dst.dpid, link.src.dpid, {'port': link.dst.port_no}) for link in links_list]

        self.net.add_edges_from(links)

    def register_router(self, dp):
        self.router[dp.id] = dp

    @set_ev_cls(dpset.EventDP, dpset.DPSET_EV_DISPATCHER)
    def datapath_handler(self, ev):
        if ev.enter:
            self.register_router(ev.dp)
