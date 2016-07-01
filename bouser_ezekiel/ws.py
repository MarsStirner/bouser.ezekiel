# -*- coding: utf-8 -*-
import datetime
import json

import blinker
from autobahn.twisted import WebSocketServerFactory, WebSocketServerProtocol
from twisted.internet.task import LoopingCall
from twisted.python import log

from bouser.helpers.plugin_helpers import BouserPlugin, Dependency
from bouser.utils import as_json
from bouser_ezekiel.service import LockAlreadyAcquired, LockNotFound

__author__ = 'viruzzz-kun'


client_connected = blinker.signal('bouser.ezekiel.ws:client.connected')
client_disconnected = blinker.signal('bouser.ezekiel.ws:client.disconnected')
client_function_called = blinker.signal('bouser.ezekiel.ws:function.called')
client_subscribed = blinker.signal('bouser.ezekiel.ws:subscribed')
client_unsubscribed = blinker.signal('bouser.ezekiel.ws:unsubscribed')

ezekiel_lock_acquired = blinker.signal('bouser.ezekiel:lock.acquired')
ezekiel_lock_released = blinker.signal('bouser.ezekiel:lock.released')


def get_cookies(headers):
    cookie_header = headers.get(u'cookie')
    if not cookie_header:
        return {}

    received_cookies = dict(
        cook.lstrip().split(b'=', 1)
        for cook in cookie_header.split(';')
    )

    return received_cookies


class EzekielWebSocketProtocol(WebSocketServerProtocol):
    """
    @type factory: EzekielWebSocketFactory
    """
    factory = None
    ping_period = 30

    def __init__(self):
        super(EzekielWebSocketProtocol, self).__init__()
        self._pinger_lc = LoopingCall(self._pinger_func)
        self.cookies = {}
        self.locks = {}
        self.waiting_locks = set()
        self.user_id = None

    def _pinger_func(self):
        self.sendEvent('ping', datetime.datetime.utcnow())

    def _authenticate(self, cookies):
        def _cb_set_user_id(user_id):
            self.user_id = user_id
            return user_id

        cas = self.factory.cas
        cookie_name = cas.cookie_name
        user_token = cookies.get(cookie_name) or ''
        return self.factory.cas.get_user_id(user_token.decode('hex')).addCallback(_cb_set_user_id)

    def sendObject(self, o):
        self.sendMessage(as_json(o))

    def sendEvent(self, event, data):
        json_data = as_json({
            'event': event,
            'data': data,
        })
        self.sendMessage(json_data)

    def _log(self, msg, *args, **kwargs):
        kw = {'system': u'Ezekiel:WebSocket[%s], uid=%s' % (self.peer, self.user_id)}
        kw.update(kwargs)
        if args:
            msg = msg % args
        return log.msg(msg, **kw)

    def _log_na(self, msg, *args, **kwargs):
        kw = {'system': u'Ezekiel:WebSocket[%s], uid=%s' % (self.peer, self.user_id)}
        kw.update(kwargs)
        if args:
            msg = msg % args
        return log.msg(msg, system=u'Ezekiel:WebSocket[%s]' % (self.peer,))

    def onConnect(self, request):
        """
        @type request: autobahn.websocket.types.ConnectionRequest
        """

        def _cb(result):
            self._pinger_lc.start(30, False)
            client_connected.send(self)
            ezekiel_lock_released.connect(self._retry_acquire_after_release)
            self._log('Authenticated')
            return result

        def _eb(failure):
            self.sendClose()
            self._log_na(u'Authentication failed %s', failure)
            return failure

        super(EzekielWebSocketProtocol, self).onConnect(request)
        self._log_na('Connection request')
        cookies = self.cookies = get_cookies(request.headers)
        self._authenticate(cookies).addCallbacks(_cb, _eb)

    def onClose(self, wasClean, code, reason):
        super(EzekielWebSocketProtocol, self).onClose(wasClean, code, reason)
        if self._pinger_lc.running:
            self._pinger_lc.stop()
        ezekiel_lock_released.disconnect(self._retry_acquire_after_release)
        self._release_all()
        client_disconnected.send(self)
        self._log('Disconnected')

    def onMessage(self, payload, isBinary):
        document = json.loads(payload)
        command = document.get('command')  # acquire, release, prolong
        # magic = document.get('magic')
        if command == 'acquire':
            self._acquire(document.get('object_id'))
        elif command == 'release':
            self._release(document.get('object_id'), document.get('token').decode('hex'))
        elif command == 'prolong':
            self._prolong(document.get('object_id'), document.get('token').decode('hex'))

    def _release_all(self):
        self.waiting_locks.clear()
        for lock in self.locks.values():
            try:
                self.factory.ezekiel.release_lock(lock.object_id, lock.token)
            except (LockNotFound, LockAlreadyAcquired) as exc:
                self.log.error(u'%s' % exc)
        self.locks.clear()
        self._log(u'Everything released...')

    def _acquire(self, object_id):
        try:
            lock = self.factory.ezekiel.acquire_lock(object_id, self.user_id)
        except LockAlreadyAcquired as lock:
            self.waiting_locks.add(object_id)
            self.sendEvent('rejected', lock)
            self._log(u'"%s" was rejected', object_id)
        except LockNotFound as exc:
            self.waiting_locks.discard(object_id)
            self.sendEvent('exception', exc)
            self._log(u'"%s" was not found', object_id)
        else:
            self.waiting_locks.discard(object_id)
            self.locks[object_id] = lock
            self.sendEvent('acquired', lock)
            self._log(u'"%s" was acquired', object_id)

    def _release(self, object_id, token):
        try:
            self.waiting_locks.discard(object_id)
            result = self.factory.ezekiel.release_lock(object_id, token)
        except LockNotFound as exc:
            self.sendEvent('exception', exc)
            self._log(u'"%s" was not found', object_id)
        else:
            del self.locks[object_id]
            self.sendEvent('released', result)
            self._log(u'"%s" was released', object_id)

    def _prolong(self, object_id, token):
        try:
            result = self.factory.ezekiel.prolong_tmp_lock(object_id, token)
        except (LockAlreadyAcquired, LockNotFound) as exc:
            self.sendEvent('exception', exc)
            self._log(u'"%s" was errored', object_id)
        else:
            self.sendEvent('prolonged', result)
            self._log(u'"%s" was prolonged', object_id)

    def _retry_acquire_after_release(self, lock_released):
        """
        @type lock_released: bouser_ezekiel.service.LockReleased
        @param lock_released:
        @return:
        """
        if lock_released.object_id in self.waiting_locks:
            self._acquire(lock_released.object_id)


class EzekielWebSocketFactory(WebSocketServerFactory, BouserPlugin):
    """
    @type ezekiel: bouser_ezekiel.service.EzekielService
    @type cas: bouser.castiel.service.CastielService
    """
    signal_name = 'bouser.ezekiel.ws'
    ezekiel = Dependency('bouser.ezekiel')
    cas = Dependency('bouser.castiel')

    protocol = EzekielWebSocketProtocol

    def buildProtocol(self, addr):
        p = self.protocol()
        p.factory = self
        p.ezekiel = self.ezekiel
        return p


def make(config):
    return EzekielWebSocketFactory()
