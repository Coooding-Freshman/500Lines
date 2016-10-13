# coding = gbk
from collections import namedtuple
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

class Node(object):
    unique_ids=itertools.count()
    def __init(self, network, address):
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
        handler_name="do_%s"%type(message).__name__
        for comp in self.roles[:]:
            if not hasattr(comp, handler_name):
                continue
            comp.logger.debug("received %s from %s", message, sender)
            fn=getattr(comp, handler_name)
            fn(sender=sender, **message._asdict())

class Role(object):
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

    def do_join(self, sender):
        if sender in self.peers:
            self.node.send([sender], Welcome(state=self.state, slot=self.slot, decisions=self.decisions))

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

