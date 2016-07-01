#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import uuid
import time

from twisted.application.service import Service
from twisted.python import log
from zope.interface import implementer
import blinker

from bouser.helpers.plugin_helpers import BouserPlugin, Dependency
from bouser.excs import SerializableBaseException
from .interfaces import ILockService, ITmpLockService

__author__ = 'viruzzz-kun'
__created__ = '13.09.2014'


ezekiel_lock_acquired = blinker.signal('bouser.ezekiel:lock.acquired')
ezekiel_lock_released = blinker.signal('bouser.ezekiel:lock.released')


class Lock(object):
    __slots__ = ['object_id', 'acquire_time', 'expiration_time', 'token', 'locker']

    def __init__(self, object_id, acquire_time, expiration_time, token, locker):
        self.object_id = object_id
        self.acquire_time = int(acquire_time)
        self.expiration_time = int(expiration_time) if isinstance(expiration_time, float) else expiration_time
        self.token = token
        self.locker = locker

    def __json__(self):
        return {
            'success': True,
            'object_id': self.object_id,
            'acquire': self.acquire_time,
            'expiration': self.expiration_time,
            'token': self.token.encode('hex'),
            'locker': self.locker,
        }


class LockAlreadyAcquired(SerializableBaseException):
    __slots__ = ['object_id', 'acquire_time', 'locker', 'message']

    def __init__(self, lock):
        self.object_id = lock.object_id
        self.acquire_time = lock.acquire_time
        self.locker = lock.locker
        self.message = u'Object "%s" already locked by %s' % (lock.object_id, lock.locker)

    def __json__(self):
        return {
            'success': False,
            'object_id': self.object_id,
            'acquire': self.acquire_time,
            'locker': self.locker,
            'exception': self.__class__.__name__,
            'message': self.message,
        }


class LockNotFound(SerializableBaseException):
    __slots__ = ['object_id', 'message']

    def __init__(self, object_id):
        self.object_id = object_id
        self.message = 'Lock not found for object "%s"' % object_id

    def __json__(self):
        return {
            'success': False,
            'object_id': self.object_id,
            'exception': self.__class__.__name__,
            'message': self.message,
        }


class LockInfo(object):
    __slots__ = ['object_id', 'acquire_time', 'locker']

    def __init__(self, lock):
        self.object_id = lock.object_id
        self.acquire_time = lock.acquire_time
        self.locker = lock.locker

    def __json__(self):
        return {
            'object_id': self.object_id,
            'acquire': self.acquire_time,
            'locker': self.locker,
        }


class LockReleased(SerializableBaseException):
    __slots__ = ['object_id']

    def __init__(self, lock):
        self.object_id = lock.object_id

    def __json__(self):
        return {
            'success': True,
            'object_id': self.object_id
        }


@implementer(ILockService, ITmpLockService)
class EzekielService(Service, BouserPlugin):
    signal_name = 'bouser.ezekiel'
    short_timeout = 60
    long_timeout = 3600
    simargl = Dependency('bouser.simargl', optional=True)

    def __init__(self, config):
        self.short_timeout = config.get('short_timeout', 60)
        self.long_timeout = config.get('long_timeout', 3600)
        self.__locks = {}

    def __acquire_lock(self, object_id, locker, short):
        logging.info('Acquiring lock %s', object_id)
        acquired_lock = self.__locks.get(object_id)
        if acquired_lock is not None:
            if short and acquired_lock[0].locker == locker:
                return self.prolong_tmp_lock(object_id, acquired_lock[0].token)
            raise LockAlreadyAcquired(self.__locks[object_id][0])
        t = time.time()
        token = uuid.uuid4().bytes
        if short:
            from twisted.internet import reactor
            timeout = self.short_timeout if short else None
            lock = Lock(object_id, t, t + timeout, token, locker)
            delayed_call = reactor.callLater(timeout, self.release_lock, object_id, token)
        else:
            lock = Lock(object_id, t, None, token, locker)
            delayed_call = None
        self.__locks[object_id] = (lock, delayed_call)
        logging.info('Lock acquired')
        log.msg('Lock for %s acquired' % object_id, system="Ezekiel")
        ezekiel_lock_acquired.send(lock)
        return lock

    def acquire_lock(self, object_id, locker):
        return self.__acquire_lock(object_id, locker, False)

    def acquire_tmp_lock(self, object_id, locker):
        return self.__acquire_lock(object_id, locker, True)

    def release_lock(self, object_id, token):
        if object_id in self.__locks:
            lock, delayed_call = self.__locks[object_id]
            if lock.token == token:
                del self.__locks[object_id]
                if delayed_call and delayed_call.active():
                    delayed_call.cancel()

                if self.simargl:
                    message = self.simargl.Message()
                    message.topic = 'ezekiel.lock.release'
                    message.data = {
                        'object_id': object_id
                    }
                    self.simargl.inject_message(message)

                log.msg('lock for %s released' % object_id, system="Ezekiel")
                logging.info('lock for %s released', object_id)
                ezekiel_lock_released.send(lock)
                return LockReleased(lock)
            raise LockNotFound(object_id)
        raise LockNotFound(object_id)

    def prolong_tmp_lock(self, object_id, token):
        if object_id in self.__locks:
            lock, timeout = self.__locks[object_id]
            if lock.token == token:
                lock.expiration_time = time.time() + self.short_timeout
                if timeout:
                    timeout.reset(self.short_timeout)
                return lock
            raise LockAlreadyAcquired(lock)
        raise LockNotFound(object_id)

