# -*- coding: utf-8 -*-
from twisted.web.resource import Resource

from bouser.helpers.plugin_helpers import Dependency, BouserPlugin

__author__ = 'viruzzz-kun'


class EzekielResource(Resource, BouserPlugin):
    web = Dependency('bouser.web')
    es = Dependency('bouser.ezekiel.eventsource', optional=True)
    rpc = Dependency('bouser.ezekiel.rest', optional=True)

    @web.on
    def web_on(self, web):
        web.root_resource.putChild('ezekiel', self)

    @es.on
    def es_on(self, es):
        self.putChild('es', es)

    @rpc.on
    def rpc_on(self, rpc):
        self.putChild('rpc', rpc)


def make(config):
    return EzekielResource()
