#!/usr/bin/env python
# -*- coding: utf-8 -*-
from twisted.internet import defer
from twisted.web.resource import IResource, Resource
from zope.interface import implementer

from bouser.excs import Unauthorized, UnknownCommand
from bouser.helpers.plugin_helpers import BouserPlugin, Dependency
from bouser.utils import api_method
from .interfaces import IRestService

__author__ = 'viruzzz-kun'
__created__ = '05.10.2014'


@implementer(IResource, IRestService)
class EzekielRestResource(Resource, BouserPlugin):
    signal_name = 'bouser.ezekiel.rest'
    isLeaf = True

    service = Dependency('bouser.ezekiel')
    cas = Dependency('bouser.castiel')
    web = Dependency('bouser.web')

    @api_method
    @defer.inlineCallbacks
    def render(self, request):
        """
        :type request: bouser.web.request.BouserRequest
        :param request:
        :return:
        """
        if self.web.crossdomain(request, True):
            defer.returnValue('')

        request.setHeader('Content-Type', 'application/json; charset=utf-8')
        pp = filter(None, request.postpath)
        if len(pp) == 2:
            command, object_id = pp
            if command == 'acquire':
                locker_id = yield self.cas.request_get_user_id(request)
                if not locker_id:
                    request.setResponseCode(403)
                    raise Unauthorized()
                result = yield self.acquire_tmp_lock(object_id, locker_id)
                defer.returnValue(result)
            elif command == 'prolong':
                token = request.args.get('token', [''])[0]
                result = yield self.prolong_tmp_lock(object_id, token.decode('hex'))
                defer.returnValue(result)
            elif command == 'release':
                token = request.args.get('token', [''])[0]
                result = yield self.release_lock(object_id, token.decode('hex'))
                defer.returnValue(result)
            else:
                raise UnknownCommand(command)
        else:
            request.setResponseCode(404)

    def acquire_tmp_lock(self, object_id, locker):
        return self.service.acquire_tmp_lock(object_id, locker)

    def prolong_tmp_lock(self, object_id, token):
        return self.service.prolong_tmp_lock(object_id, token)

    def release_lock(self, object_id, token):
        return self.service.release_lock(object_id, token)


def make(config):
    return EzekielRestResource()
