# -*- coding: utf-8 -*-

from __future__ import absolute_import
from datetime import datetime


class TimestampProvider(object):
    @staticmethod
    def get_now():
        return datetime.today().timestamp() * 1000  # seconds -> milliseconds
