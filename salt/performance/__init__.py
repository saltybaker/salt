# -*- coding: utf-8 -*-

from __future__ import absolute_import
import logging

__virtualname__ = 'log'

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9900
CONFIG_ZMQ_HOST = 'zmq_host'
CONFIG_ZMQ_PORT = 'zmq_port'

logger = logging.getLogger(__name__)


def init(hub):
    hub.performance.log.active = False
    opts = hub.opts

    perf = opts.get('performance', None)
    if not isinstance(perf, dict):
        logger.warning('No configuration for performance module - inactive')
        return

    agg = perf.get('aggregator')

    if (not isinstance(agg, dict) or
        agg.get(CONFIG_ZMQ_HOST) is None or
        agg.get(CONFIG_ZMQ_PORT) is None):
        logmsg = ('No configuration for performance aggregator ' +
                  '- using defaults  host: {} ' +
                  'port: {}').format(DEFAULT_HOST, DEFAULT_PORT)
        logger.warning(logmsg)
        agg = {CONFIG_ZMQ_HOST: DEFAULT_HOST, CONFIG_ZMQ_PORT: DEFAULT_PORT}

    zmq_host = agg.get(CONFIG_ZMQ_HOST, DEFAULT_HOST)
    zmq_port = agg.get(CONFIG_ZMQ_PORT, DEFAULT_PORT)
    hub.performance.log.active = True
