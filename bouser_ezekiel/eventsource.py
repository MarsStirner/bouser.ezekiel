#!/usr/bin/env python
# -*- coding: utf-8 -*-
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from twisted.python import log
from twisted.python import failure
from twisted.web.resource import IResource, Resource
from twisted.web.server import NOT_DONE_YET
from zope.interface import implementer

from bouser.helpers.eventsource import make_event
from bouser.helpers.plugin_helpers import BouserPlugin, Dependency
from bouser.utils import safe_int
from .interfaces import IRestService

__author__ = 'viruzzz-kun'
__created__ = '05.10.2014'


class EventSourcedLock(object):
    keep_alive = False

    def __init__(self, object_id, locker, request, ezekiel):
        self.object_id = object_id
        self.locker = locker
        self.request = request
        self.ezekiel = ezekiel
        self.lock = None
        self.lc = None
        self.ka = LoopingCall(self.request.write, make_event(None, 'ping'))

    def try_acquire(self):
        lock = self.ezekiel.acquire_lock(self.object_id, self.locker)
        if hasattr(lock, 'token'):
            self.lock = lock
            self.request.write(make_event(lock, 'acquired'))
            self.lc.stop()
            self.lc = LoopingCall(self.try_prolong)
            self.lc.start(self.ezekiel.long_timeout / 2, False)
        else:
            self.request.write(make_event(lock, 'rejected'))

    def try_prolong(self):
        try:
            self.lock = self.ezekiel.prolong_tmp_lock(self.object_id, self.lock.token)
        except Exception as e:
            self.request.write(make_event(e, 'exception'))
            self.lock = None
            self.stop()
            self.request.finish()
        else:
            self.request.write(make_event(self.lock, 'prolonged'))

    def start(self):
        if isinstance(self.keep_alive, int):
            self.ka.start(self.keep_alive)
        self.lc = LoopingCall(self.try_acquire)
        self.lc.start(10)

    def stop(self):
        if self.lc:
            self.lc.stop()
        if self.ka.running:
            self.ka.stop()
        if self.lock:
            self.ezekiel.release_lock(self.object_id, self.lock.token)


@implementer(IResource, IRestService)
class EzekielEventSourceResource(Resource, BouserPlugin):
    signal_name = 'bouser.ezekiel.eventsource'
    isLeaf = True

    service = Dependency('bouser.ezekiel')
    cas = Dependency('bouser.castiel')
    web = Dependency('bouser.web')

    def __init__(self, config):
        Resource.__init__(self)
        self.keep_alive = safe_int(config.get('keep-alive', False))

    @defer.inlineCallbacks
    def render(self, request):
        """
        :type request: bouser.web.request.BouserRequest
        :param request:
        :return:
        """
        # TODO: Test it
        self.web.crossdomain(request, True)

        pp = filter(None, request.postpath)
        if len(pp) != 1:
            request.setResponseCode(404, 'Resource not found')
            defer.returnValue('')
        object_id = pp[0]

        def onFinish(result):
            if ezl:
                ezl.stop()
            log.msg(
                u'Connection from %s closed. "%s" hopefully released' % (request.getClientIP(), object_id),
                system="Ezekiel Event Source")
            if not isinstance(result, failure.Failure):
                return result

        user_id = yield self.cas.request_get_user_id(request)

        if not user_id:
            request.setResponseCode(401, 'Authentication Failure')
            defer.returnValue('')
        else:
            request.user = user_id
            request.setHeader('Content-Type', 'text/event-stream; charset=utf-8')

            ezl = EventSourcedLock(object_id, user_id, request, self.service)
            ezl.keep_alive = self.keep_alive
            request.notifyFinish().addBoth(onFinish)
            ezl.start()
            log.msg(
                u'Connection from %s established. Locking "%s"' % (request.getClientIP(), object_id),
                system="Ezekiel Event Source")

        defer.returnValue(NOT_DONE_YET)


def make(config):
    return EzekielEventSourceResource(config)
