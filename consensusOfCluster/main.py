from Consensus import *
import sys

def key_value_state_machine(state, input_value):
    if input_value[0]=='get':
        return state, state.get(input_value[1], None)
    elif input_value[0]=='set':
        state[input_value[1]]=input_value[2]
        return state, input_value[2]

sequence_runing=0
def do_sequence(network, node, key):
    global sequence_runing
    sequence_runing+=1
    reqs=[
        (('get', key), None),
        (('set', key, 10), 10),
        (('get', key), 10),
        (('set', key, 20), 20),
        (('set', key, 30), 30)
    ]
    def request():
        if not reqs:
            global sequence_runing
            sequence_runing-=1
            if not sequence_runing:
                network.stop()
            return
        myin, exp_output=reqs.pop(0)
        def req_done(output):
            assert output == exp_output, "%r != %r" %(output, exp_output)
            request()
        Requester(node, myin, req_done).start()

    network.set_timer(None, 1.0, request)

def main():
    logging.basicConfig(format="%(name)s-%(message)s",level=logging.DEBUG)
    network=Network(int(sys.argv[1]))
    peers=["N{}".format(i) for i in range(7)]
    for p in peers:
        node=network.new_node(address=p)
    if p=='N0':
        Seed(node, initial_state={}, peers=peers, execute_fun=key_value_state_machine)
    else:
        Bootstrap(node, execute_fun=key_value_state_machine, peers=peers).start()

    for key in 'abcedfg':
        do_sequence(network, node, key)
    network.run()

if __name__ == "__main__":
    main()
