#!/usr/bin/env python3

import dnf
import dnf.base
import dnf.conf
import dnf.const
import dnf.query

import hawkey

import pydbus

from pydbus import SessionBus
from pydbus.generic import signal
from os.path import join, dirname

from gi.repository import GLib

from typing import Dict

from asyncio import Lock

class QueryKit(object):
    """
      <node>
        <interface name='com.github.Appadeia.QueryKit'>
            <method name='SearchPackages'>
              <arg type='s' name='query' direction='in'/>
              <arg type='s' name='distro' direction='in'/>
              <arg type='a(ss)' name='packages' direction='out'/>
            </method>
            <property name="Distros" type="as" access="read">
              <annotation name="org.freedesktop.DBus.Property.EmitsChangedSignal" value="false"/>
            </property>
        </interface>
      </node>
    """

    _dnf_objects: Dict[str,dnf.Base] = {"fedora": dnf.Base(), "opensuse": dnf.Base(), "openmandriva": dnf.Base(), "mageia": dnf.Base()}
    _lockdict: Dict[str,Lock]

    def SearchPackages(self, query, distro):
        if distro not in self._dnf_objects.keys():
            return [('Invalid Distro', 'This is an invalid distro.')]
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()
        available_pkgs: dnf.query.Query = available_pkgs.filter(name__substr=query,arch=["noarch","x86_64"])

        pkgs = []
        for pkg in available_pkgs:
            pkgs.append((pkg.name, pkg.summary))

        return pkgs

    @property
    def Distros(self):
        keys = []
        for key in self._dnf_objects:
            keys.append(key)
        return keys

    def RefreshPackages(self):
        for key in self._dnf_objects:
            try:
                print("Refreshing {}...".format(key))
                self._dnf_objects[key].reset(goal=True,repos=True,sack=True)
                self._dnf_objects[key].read_all_repos()
                self._dnf_objects[key].fill_sack(load_system_repo=False)
            except:
                print("Could not refresh {}.".format(key))
            print("Refreshed {}!".format(key))

    def __init__(self):
        print("Loading repos...")

        arch = hawkey.detect_arch()

        to_pop = []
        for key in self._dnf_objects:
            print("Loading {}...".format(key))
            try:
                self._dnf_objects[key].conf.gpgcheck = False
                self._dnf_objects[key].conf.substitutions['arch'] = arch
                self._dnf_objects[key].conf.substitutions['basearch'] = dnf.rpm.basearch(arch)
                if key == "fedora":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '30'
                if key == "openmandriva":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '4.0'
                if key == "mageia":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '7.1'
                self._dnf_objects[key].conf.zchunk = False
                self._dnf_objects[key].conf.reposdir = join(dirname(__file__), key)
                self._dnf_objects[key].conf.cachedir = join(dirname(__file__), "cache", key)
                self._dnf_objects[key].read_all_repos()
                self._dnf_objects[key].fill_sack(load_system_repo=False)
            except Exception as e:
                print("Failed to load {}!".format(key))
                print("Error:\n>>>\t".format(e))
                to_pop.append(key)
                continue
            print("Loaded {}!".format(key))
        
        for i in to_pop:
            self._dnf_objects.pop(i)

        GLib.timeout_add_seconds(60, self.RefreshPackages)
        print("Repos loaded!")

loop = GLib.MainLoop()
bus = SessionBus()

bus.publish("com.github.Appadeia.QueryKit", QueryKit())
loop.run()