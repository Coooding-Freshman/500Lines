#-*- coding:UTF-8 -*-
from collections import namedtuple
import copy
import random
import heapq
import Queue
import threading
import itertools
import logging
import functools

Accepted = namedtuple('Accepted', ['slot', 'ballot_num'])
Accept = namedtuple('Accept', ['slot', 'ballot_num', 'proposal'])
Decision = namedtuple('Decision', ['slot', 'proposal'])
Invoked = namedtuple('Invoked', ['client_id', 'output'])
Invoke = namedtuple('Invoke', ['caller', 'client_id', 'input_value'])
Join = namedtuple('Join', [])
Active = namedtuple('Active', [])
Prepare = namedtuple('Prepare', ['ballot_num'])
Promise = namedtuple('Promise', ['ballot_num', 'accepted_proposals'])
Propose = namedtuple('Propose', ['slot', 'proposal'])
Welcome = namedtuple('Welcome', ['state', 'slot', 'decisions'])
Decided = namedtuple('Decided', ['slot'])
Preempted = namedtuple('Preempted', ['slot', 'preempted_by'])
Adopted = namedtuple('Adopted', ['ballot_num', 'accepted_proposals'])
Accepting = namedtuple('Accepting', ['leader'])

Proposal = namedtuple('Proposal', ['caller', 'client_id', 'input'])
Ballot = namedtuple('Ballot', ['n', 'leader'])

JOIN_RETRANSMIT = 0.7
CATCHUP_INTERVAL = 0.6
ACCEPT_RETRANSMIT = 1.0
PREPARE_RETRANSMIT = 1.0
INVOKE_RETRANSMIT = 0.5
LEADER_TIMEOUT = 1.0
NULL_BALLOT = Ballot(-1, -1)  # sorts before all real ballots
NOOP_PROPOSAL = Proposal(None, None, None)  # no-op to fill otherwise empty slots

class SimTimeLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "T=%.3f %s" % (self.extra['network'].now, msg), kwargs

    def getChild(self, name):
        return self.__class__(self.logger.getChild(name), {'network':
                                                    self.extra['network']})

class Node(object):
    '''将cluster里面的所有role聚合在一起的一个类'''
    unique_ids=itertools.count()
    def __init__(self, network, address):
        self.network=network
        self.address=address or 'N%d' % self.unique_ids.next()
        self.logger=SimTimeLogger(logging.getLogger(self.address), {'network': self.network})
        self.logger.info("starting")
        self.roles=[]
        self.send=functools.partial(self.network.send, self)

    def register(self, role):
        self.roles.append(role)

    def unregister(self, role):
        self.roles.remove(role)

    def receive(self, sender, message):
        '''一个很重要的函数, 为什么用namedtuple, 接受信息后怎么应该怎么做'''
        handler_name="do_%s"%type(message).__name__
        for comp in self.roles[:]:
            if not hasattr(comp, handler_name):
                continue
            comp.logger.debug("received %s from %s", message, sender)
            fn=getattr(comp, handler_name)
            fn(sender=sender, **message._asdict())

class Role(object):
    '''所有角色的基类'''
    def __init__(self, node):
        self.node=node
        self.node.register(self)
        self.running=True
        self.logger=node.logger.getChild(type(self).__name__)

    def set_timer(self, second, callbcak):
        return self.node.network.set_timer(self.node.address, second, lambda :self.running or callable())

    def stop(self):
        self.running=False
        self.node.unregister(self)

class Accepter(Role):
    def __init__(self, node):
        super(Accepter, self).__init__(node)
        self.ballot_num=NULL_BALLOT
        self.accepted_proposals={}

    def do_Prepare(self, sender, ballot_num):
        '''如果大于自己的选举编号投赞成票, 两次发送一个是给leader的, 一个
        给replica 的'''
        if ballot_num>self.ballot_num:
            self.ballot_num=ballot_num
            self.node.send([self.node.address], Accepting(leader=sender))

        self.node.send([sender], Promise(ballot_num=self.ballot_num, accepted_proposals=self.accepted_proposals))

    def do_Accept(self, sender, ballot_num, slot, proposal):
        if ballot_num>=self.ballot_num:
            self.ballot_num=ballot_num
            acc=self.accepted_proposals
            if slot not in acc or acc[slot][0]<ballot_num:
                acc[slot]={ballot_num, proposal}

        self.node.send([sender], Accepted(slot=slot, ballot_num=self.ballot_num))

class Replica(Role):
    def __init__(self, node, execute_fn, state, slot, decisions, peers):
        super(Replica, self).__init__(node)
        self.state=state
        self.execute_fn=execute_fn
        self.slot=slot
        self.decisions=decisions
        self.peers=peers
        self.proposals=[]
        self.next_slot=slot
        self.latest_leader=None
        self.latest_leader_timeout=None

    def do_Inovke(self, sender, caller, client_id, input_value):
        proposal=Proposal(caller, client_id, input_value)
        slot=next((s for s, p in self.proposals.iteritems() if p==proposal), None)
        self.propose(proposal, slot)

    def propose(self, proposal, slot=None):
        if not slot:
            slot, self.next_slot=self.next_slot, self.next_slot+1
        self.proposals[slot]=proposal
        leader=self.latest_leader or self.node.address
        self.logger.info("proposing %s at slot %d to leader %s"%(proposal, slot, leader))
        self.node.send([leader], Proposal(slot=slot, proposal=proposal))

    def do_Decision(self, sender, slot, proposal):
        assert not self.decision.get(self.slot, None), "next slot to commit is already decided"
        if slot in self.decisions:
            assert self.decisions[slot]==proposal, "slot %d already decision with %r "%(slot, self.decisions[slot])
            return
        self.decisions[slot]=proposal
        self.next_slot=max(self.next_slot,slot+1)

        our_proposal=self.proposals.get(slot)
        if(our_proposal is not None and our_proposal!=proposal and our_proposal.caller):
            self.propose(our_proposal)
        while True:
            commit_proposal = self.decisions.get(slot)
            if not commit_proposal:
                break
            commit_slot, self.slot=self.slot, self.slot+1
            self.commit(commit_slot, commit_proposal)

    def  commit(self, slot, proposal):
        decided_proposal=[p for s, p in self.decisions.iteritems() if s<slot]
        if proposal in decided_proposal:
            self.logger.info("not commit duplicate proposal %r,slot %d"%proposal, slot)
            return
        self.logger.info("committing %r at slot %d"%proposal, slot)
        if proposal.caller is not None:
            self.state, output=self.execute_fn(self.state, proposal.input)
            self.node.send([proposal.caller], Invoked(client_id=proposal.client, output=output))

    def do_Adopted(self, sender, ballot_num, accepted_proposals):
        self.latest_leader=self.node.address
        self.leader_alive()

    def do_Acceptint(self, sender, leader):
        self.latest_leader=leader
        self.leader_alive()

    def do_Active(self, sender):
        if sender!=self.latest_leader:
            return
        self.leader_alive()

    def leader_alive(self):
        if self.latest_leader_timeout:
            self.latest_leader_timeout.cancel()
        def reset_leader():
            idx=self.peers.index(self.latest_leader)
            self.latest_leader=self.peers[(idx+1)%len(self.peers)]
            self.logger.debug("leader timed out; tring the next one, %s", self.latest_leader)
        self.latest_leader_timeout=self.set_timer(LEADER_TIMEOUT, reset_leader())

    def do_Join(self, sender):
        if sender in self.peers:
            self.node.send([sender], Welcome(state=self.state, slot=self.slot, decisions=self.decisions))

class Commander(Role):
    def __init__(self, node, ballot_num, slot, proposal, peers):
        super(Commander, self).__init__(node)
        self.ballot_num=ballot_num
        self.slot=slot
        self.proposal=proposal
        self.acceptors=set([])
        self.peers=peers
        self.quorum=len(peers)+1

    def start(self):
        self.node.send(set(self.peers)-self.acceptors, Accept(slot=self.slot, ballot_num=self.ballot_num, proposal=self.proposal))
        self.set_timer(ACCEPT_RETRANSMIT, self.start)

    def finished(self, ballot_num, preempted):
        if preempted:
            self.node.send([self.node.address], Preempted(slot=self.slot, preempted_by=ballot_num))
        else:
            self.node.send([self.node.address], Decided(slot=self.slot))
        self.stop()

    def do_Accepted(self, sender, slot, ballot_num):
        if self.slot!=slot:
            return
        if ballot_num==self.ballot_num:
            self.acceptors.add(sender)
            if len(self.acceptors)<self.quorum:
                return
            self.node.send(self.peers, Decision(slot=self.slot, proposal=self.proposal))
            self.finished(ballot_num, False)
        else:
            self.finished(ballot_num, True)

class Scout(Role):
    def __init__(self, node, ballot_num, peers):
        super(Scout, self).__init__(node)
        self.ballot_num=ballot_num
        self.accepted_proposals={}
        self.acceptors=set([])
        self.peers=peers
        self.quorum=len(peers)+1
        self.retransmit_timer=None

    def start(self):
        self.logger.info("scout starting")
        self.send_prepare()

    def send_prepare(self):
        self.node.send(self.peers, Prepare(ballot_num=self.ballot_num))
        self.retransmit_timer=self.set_timer(PREPARE_RETRANSMIT, self.send_prepare)

    def update_accepted(self, accepted_proposals):
        acc=self.accepted_proposals
        for slot, (ballot_num, proposal) in acc.iteritems():        #一次投票可能有好几个proposal 所以这要遍历一个集合
            if slot not in acc or acc[slot][0]<ballot_num:
                acc[slot]=(ballot_num, proposal)

    def do_Promise(self, sender, ballot_num, accepted_proposals):
        if ballot_num==self.ballot_num:
            self.logger.info("got matching promise: need %d" % self.quorum)
            self.update_accepted(accepted_proposals)
            self.acceptors.add(sender)
            if len(self.acceptors)>=self.quorum:
                accepted_proposals=dict((s,p) for s, (b,p) in self.accepted_proposals.iteritems())
                self.node.send([self.node.address], Adopted(ballot_num=ballot_num, accepted_proposals=accepted_proposals))
                self.stop()
        else:
            self.node.send([self.node.address],Preempted(slot=None, preempted_by=ballot_num))
            self.stop()

class Leader(Role):
    def __init__(self, node, peers, commander_cls=Commander, scout_cls=Scout):
        super(Leader, self).__init__(node)
        self.ballot_num=Ballot(0, node.address)
        self.active=False
        self.proposals={}
        self.commander_cls=commander_cls
        self.scout_cls=scout_cls
        self.scouting=False
        self.peers=peers

    def start(self):
        def active():
            if self.active:
                self.node.send(self.peers, Active())
            self.set_timer(LEADER_TIMEOUT/2.0, active)
        active()

    def spawn_scout(self):
        assert not self.scouting
        self.scouting=True
        self.scout_cls(self.node, self.ballot_num, self.peers).start()

    def do_Adoptd(self, sender, ballot_num, accepted_proposals):
        self.scouting=False
        self.proposals.update(accepted_proposals)
        self.logger.info("leader becoming active")
        self.active=True

    def spawn_commander(self, ballot_num, slot):
        proposal=self.proposals[slot]
        self.commander_cls(self.node, ballot_num, slot, proposal, self.peers).start()

    def do_Preempted(self, sender, slot, preempted_by):
        if not slot:
            self.scouting=False
        self.logger.info("leader preempted by %s", preempted_by.leader)
        self.active=False
        self.ballot_num=Ballot((preempted_by or self.ballot_num).n+1, self.ballot_num.leadr)

    def do_Propose(self, sender, slot, proposal):
        if slot not in self.proposals:
            if self.active:
                self.proposals[slot]=proposal
                self.logger.info("spawning commander for slot %d"%(slot,))
                self.spawn_commander(self.ballot_num, slot)
            else:
                if not self.scounting:
                    self.logger.info("got PROPOSE when not active - scouting" )
                    self.spawn_scout()
                else:
                    self.logger.info("got PROPOSE while scouting: ignored")
        else:
            self.logger.info("got PROPOSE for a slot already being proposed")

class Bootstrap(Role):
    def __init__(self, node, peers, execute_fun, replica_cls=Replica,
                 acceptor_cls=Accept, commander_cls=Commander, scout_cls=Scout):
        super(Bootstrap, self).__init__(node)
        self.execute_fun=execute_fun
        self.peers=peers
        self.peers_cycle=itertools.cycle(peers)
        self.replica_cls=replica_cls
        self.acceptor_cls=acceptor_cls
        self.commander_cls=commander_cls
        self.scout_cls=scout_cls

    def start(self):
        self.join()

    def join(self):
        self.node.send([next(self.peers_cycle)], Join())
        self.set_timer(JOIN_RETRANSMIT, self.join)

    def do_Welcome(self, sender, state, slot, decisions):
        self.acceptor_cls(self.node)
        self.replica_cls(self.node, execute_fun=self.execute_fun,
                         peers=self.peers, state=state, slot=slot, decisions=decisions)
        self.leader_cls(self.node, peers=self.peers, commander_cls=self.commander_cls,
                        scout_cls=self.scout_cls).start()
        self.stop()

class Seed(Role):
    def __init__(self, node, initial_state, execute_fn, peers,
                 bootstrap_cls=Bootstrap):
        super(Seed, self).__init__(node)
        self.execute_fn=execute_fn
        self.peers=peers
        self.bootstrap_cls=bootstrap_cls
        self.seen_peers=set([])
        self.exit_timer=None
        self.initial_state=initial_state

    def do_Join(self, sender):
        self.seen_peers.add(sender)
        if len(self.seen_peers)<=len(self.peers)/2:
            return
        self.node.send(list(self.seen_peers), Welcome(state=self.initial_state, slot=1, decisions={}))
        if self.exit_timer:
            self.exit_timer.cancel()
        self.exit_timer=self.set_timer(JOIN_RETRANSMIT*2, self.finish)

    def finish(self):
        bs=self.bootstrap_cls(self.node, peers=self.peers, execute_fn=self.execute_fn)
        bs.start()
        self.stop()

class Requester(Role):
    client_ids=itertools.count(start=100000)
    def __init__(self, node, n, callback):
        super(Requester, self).__init__(node)
        self.client_id=self.client_ids.next()
        self.n=n
        self.output=None
        self.callback=callback

    def start(self):
        self.node.send([self.node.address], Invoke(caller=self.node.address,
                                        client_id=self.client_id,input_value=self.n))
        self.invoke_timer=self.set_timer(INVOKE_RETRANSMIT, self.start)

    def do_Invoked(self, sender, client_id, output):
        if client_id!=self.client_id:
            return
        self.logger.debug("received output %r" % (output,))
        self.invoke_timer.cancel()
        self.callback(output)
        self.stop()

class Member(object):
    '''应用程序接口, '''
    def __init__(self, state_machine, network, peers,
                 seed=None, seed_cls=Seed, bootstrap_cls=Bootstrap):
        self.network=network
        self.node=network.new_node()
        if seed is not None:
            self.startup_role=seed_cls(self.node, initial_state=seed, peers=peers, execute_fn=state_machine)
        else:
            self.startup_role=bootstrap_cls(self.node, execute_fn=state_machine, peers=peers)
        self.requester=None

    def start(self):
        self.startup_role.start()
        self.thread=threading.Thread(target=self.network.run)
        self.thread.start()

    def invoke(self, input_value, request_cls=Requester):
        "产生了一个Replica object 并且返回一个Replica object"
        assert self.requester is None
        q=Queue.Queue()
        self.requester=request_cls(self.node, input_value, q.put)
        self.requester.start()
        output=q.get()
        self.requester=None
        return output

class Timer(object):
    def __init__(self, expires, address, callback):
        self.expires=expires
        self.address=address
        self.callback=callback
        self.cancelled=False

    def __cmp__(self, other):
        return cmp(self.expires, other.expires)

    def cancel(self):
        self.cancelled=True

class Network(object):
    PROP_DELAY=0.03
    PROP_JITTER=0.02
    DROP_PROB=0.05

    def __init__(self, seed):
        self.nodes={}
        self.rnd=random.Random(seed)
        self.timers=[]
        self.now=1000.0

    def new_node(self, address=None):
        node=Node(self, address=address)
        self.nodes[node.address]=node
        return node

    def run(self):
        while self.timers:
            next_timer=self.timers[0]
            if next_timer.expires>self.now:
                self.now=next_timer.expires
            heapq.heappop(self.timers)
            if next_timer.cancelled:
                continue
            if not next_timer.address or next_timer.address in self.nodes:
                next_timer.callback()

    def stop(self):
        self.timers=[]

    def set_timer(self, address, seconds, callback):
        timer=Timer(self.now+seconds, address, callback)
        heapq.heappush(self.timers, timer)
        return timer

    def send(self, sender, destinations, message):
        sender.logger.debug("sending %s to %s", message, destinations)
        def sendto(dest, message):
            if dest == sender.address:
                self.set_timer(sender.address, 0, lambda: sender.receive(sender.address, message))
            elif self.rnd.uniform(0, 1.0) > self.DROP_PROB:
                delay = self.PROP_DELAY + self.rnd.uniform(-self.PROP_JITTER, self.PROP_JITTER)
                self.set_timer(dest, delay, functools.partial(self.nodes[dest].receive,
                                                              sender.address, message))
        for dest in (d for d in destinations if d in self.nodes):
            sendto(dest, copy.deepcopy(message))


