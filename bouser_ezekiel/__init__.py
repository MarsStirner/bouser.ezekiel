#!/usr/bin/env python
# -*- coding: utf-8 -*-
from . import interfaces, service, rest, eventsource, web


__author__ = 'viruzzz-kun'
__created__ = '13.09.2014'


def make(config):
    return service.EzekielService(config)
