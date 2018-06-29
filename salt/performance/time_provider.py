# -*- coding: utf-8 -*-

from __future__ import absolute_import
import time


class TimestampProvider(object):
    @staticmethod
    def get_now():
        return time.time() * 1000  # seconds -> milliseconds
