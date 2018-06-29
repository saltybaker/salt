# -*- coding: utf-8 -*-
'''
A salt engine which retransmits the messages from salt event bus to another ZMQ channel.
'''

# Import python libs
from __future__ import absolute_import, print_function, unicode_literals

import json
import logging

# Import salt libs
import time
import zmq

import salt.utils.event
import salt.utils.json
from salt.performance.payloads import message_observed
from salt.performance.time_provider import TimestampProvider

log = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9900
CONFIG_ZMQ_HOST = 'zmq_host'
CONFIG_ZMQ_PORT = 'zmq_port'


class JsonRenderer(object):
    def marshal(self, smth):
        return json.dumps(smth)


# FIXME [KN] refactor to use salt.transport.zmq instead
class ZmqSender(object):
    def __init__(self, json_renderer, zmq_host='localhost', zmq_port=9900):
        self.json_renderer = json_renderer
        log.debug("ZMQ version: %s", zmq.zmq_version())

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDBUF, 102400)
        self.socket.setsockopt(zmq.LINGER, 2000)

        self.socket.set_hwm(10240)

        zmq_address = "tcp://{}:{}".format(zmq_host, zmq_port)
        log.info("Connecting to ZMQ at address: %s", zmq_address)

        self.socket.connect(zmq_address)
        time.sleep(2)

    def send(self, payload):
        out_str = self.json_renderer.marshal(payload)
        log.debug("Sent message: %s", out_str)
        self.socket.send_unicode("{} {}".format("topic", out_str))

    def close(self):
        self.socket.close()
        self.context.destroy(linger=3000)


def start(host=DEFAULT_HOST,
          port=DEFAULT_PORT):
    '''
    Listen to events and resend the message queue throughput statistics to the Aggregator.
    '''

    perf = __opts__.get('performance', None)
    if not isinstance(perf, dict):
        log.warning('engine::relay: No configuration for performance module - inactive')
        return
    else:
        log.debug('engine::relay: Performance module is NOT enabled')

    agg = perf.get('aggregator')

    if (not isinstance(agg, dict) or
        agg.get(CONFIG_ZMQ_HOST) is None or
        agg.get(CONFIG_ZMQ_PORT) is None):
        logmsg = ('No configuration for performance aggregator ' +
                  '- using defaults  host: {} ' +
                  'port: {}').format(DEFAULT_HOST, DEFAULT_PORT)
        log.warning(logmsg)
        agg = {CONFIG_ZMQ_HOST: DEFAULT_HOST, CONFIG_ZMQ_PORT: DEFAULT_PORT}

    zmq_host = agg.get(CONFIG_ZMQ_HOST, host)
    zmq_port = int(agg.get(CONFIG_ZMQ_PORT, port))

    if __opts__['__role'] == 'master':
        event_bus = salt.utils.event.get_master_event(
            __opts__,
            __opts__['sock_dir'],
            listen=True)
    else:
        event_bus = salt.utils.event.get_event(
            'minion',
            transport=__opts__['transport'],
            opts=__opts__,
            sock_dir=__opts__['sock_dir'],
            listen=True)
    log.debug("engine::relay: - Performance using host=%s and port=%d for ZMQ", zmq_host, zmq_port)
    sender = ZmqSender(JsonRenderer(), zmq_host=zmq_host, zmq_port=zmq_port)
    log.debug('ZMQ relay engine has started')

    try:
        while True:
            event = event_bus.get_event(full=True)
            if not event:
                continue

            # from pudb.remote import set_trace; set_trace(term_size=(200, 48), port=6898)

            jevent = salt.utils.json.dumps(event)
            sender.send(message_observed(msg_length=len(jevent), tag=event['tag'], ts=TimestampProvider.get_now()))

            if event['tag'].startswith('perf/'):
                sender.send(event['data']['data'])

    finally:
        sender.close()
        log.debug("ZMQ relay engine has stopped")
