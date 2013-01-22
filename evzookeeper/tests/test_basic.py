# vim: tabstop=4 shiftwidth=4 softtabstop=4
# Copyright 2012 AT&T. All rights reserved.

import logging
import unittest

import zookeeper

from evzookeeper import ZKSession
from evzookeeper import ZOO_OPEN_ACL_UNSAFE

logging.basicConfig(level=logging.DEBUG)


class BasicTestCase(unittest.TestCase):

    def setUp(self):
        self.session = ZKSession("localhost:2181", timeout=10)

    def test_recursive_ops(self):
        ## path /a/b/c/d
        self.session.ensure_path('/a/b/c/d', 'hello',
                                 acl=[ZOO_OPEN_ACL_UNSAFE])
        (data, _) = self.session.get('/a')
        self.assertEqual(data, 'hello')
        (data, _) = self.session.get('/a/b')
        self.assertEqual(data, 'hello')
        (data, _) = self.session.get('/a/b/c')
        self.assertEqual(data, 'hello')
        (data, _) = self.session.get('/a/b/c/d')
        self.assertEqual(data, 'hello')
        ## path /a/b/e/f
        self.session.ensure_path('/a/b/e/f', 'world',
                                 acl=[ZOO_OPEN_ACL_UNSAFE])
        (data, _) = self.session.get('/a')
        self.assertEqual(data, 'hello')
        (data, _) = self.session.get('/a/b')
        self.assertEqual(data, 'hello')
        (data, _) = self.session.get('/a/b/e')
        self.assertEqual(data, 'world')
        (data, _) = self.session.get('/a/b/e/f')
        self.assertEqual(data, 'world')
        ## intersection of the two is /a/b
        children = set(self.session.get_children('/a/b'))
        self.assertEqual(children, set(['c', 'e']))
        ## delete all
        self.session.delete('/a', recursive=True)
        ## test existence
        existed = True
        try:
            self.session.exists('/a/b/c/d')
        except zookeeper.NoNodeException:
            existed = False
        self.assertFalse(existed)
        ## test existence
        existed = True
        try:
            self.session.exists('/a/b/c')
        except zookeeper.NoNodeException:
            existed = False
        self.assertFalse(existed)
        ## test existence
        existed = True
        try:
            self.session.exists('/a/b')
        except zookeeper.NoNodeException:
            existed = False
        self.assertFalse(existed)
        ## test /a existence
        existed = True
        try:
            self.session.exists('/a')
        except zookeeper.NoNodeException:
            existed = False
        self.assertFalse(existed)
