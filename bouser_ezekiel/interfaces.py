#!/usr/bin/env python
# -*- coding: utf-8 -*-
from zope.interface import Interface
from zope.interface.interface import Attribute

__author__ = 'viruzzz-kun'
__created__ = '13.09.2014'


class ILockService(Interface):
    def acquire_lock(self, object_id, locker):
        """
        Acquire Lock until it is explicitly released
        :param object_id: Object identifier
        :param locker: Locker identifier
        :return: lock id (token)
        """

    def release_lock(self, object_id, token):
        """
        Explicitly release lock
        :param object_id: Object identifier
        :param token: associated lock identifier
        :return:
        :raise: LockNotFound
        """


class ITmpLockService(Interface):
    def acquire_tmp_lock(self, object_id, locker):
        """
        Acquire Lock until timeout
        :param object_id: Object identifier
        :param locker: Locker identifier
        :return: lock id (token)
        """

    def prolong_tmp_lock(self, object_id, token):
        """
        Prolong locking
        :param object_id: Object identifier
        :param token: Lock identifier
        :return:
        :raise: LockNotFound
        """

    def release_lock(self, object_id, token):
        """
        Explicitly release lock
        :param object_id: Object identifier
        :param token: associated lock identifier
        :return:
        :raise: LockNotFound
        """


class ILockSession(Interface):
    locker = Attribute('locker', """Description of the locker""")
    locks = Attribute('locks', """Dict of the locks""")

    def start_session(self, locker):
        """
        Provide information for session
        :type locker: dict
        :param locker: information about Locker
        :return None
        """

    def close_session(self):
        """
        Close the session
        """

    def acquire_lock(self, object_id):
        """
        Acquire lock for session lifetime
        :param object_id:
        :return:
        """

    def release_lock(self, object_id):
        """
        Release lock
        :param object_id:
        :return:
        """


class IRestService(Interface):
    def acquire_tmp_lock(self, object_id, locker):
        pass

    def prolong_tmp_lock(self, object_id, token):
        pass

    def release_lock(self, object_id, token):
        pass


class IWsLockFactory(Interface):
    def register(self, client):
        pass

    def unregister(self, client):
        pass


class ISessionLockProtocol(Interface):
    def command_acquire_lock(self, params):
        pass

    def command_release_lock(self, params):
        pass

    def command_locker(self, params):
        pass