#!/usr/bin/env python3

from os.path import join, dirname
from typing import Dict

import dnf
import dnf.base
import dnf.conf
import dnf.const
import dnf.query

import hawkey

from dbus_next.service import ServiceInterface, method, dbus_property, signal, Variant
from dbus_next.aio import MessageBus

import asyncio

class QueryKit(ServiceInterface):
    _dnf_objects: Dict[str,dnf.Base] = {"fedora": dnf.Base(), "tumbleweed": dnf.Base(), "leap": dnf.Base(), "openmandriva": dnf.Base(), "mageia": dnf.Base(), "centos": dnf.Base()}

    @method(name="SearchPackages")
    def SearchPackages(self, query: 's', distro: 's') -> 'a(sssiis)':
        if distro not in self._dnf_objects.keys():
            return [['Invalid Distro', 'This is an invalid distro.','N/A', -1, -1, 'N/A']]
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()
        available_pkgs: dnf.query.Query = available_pkgs.filter(name__substr=query,arch=["noarch","x86_64"])

        pkgs = []
        for pkg in available_pkgs:
            pkgs.append([pkg.name, pkg.summary, pkg.version, pkg.downloadsize, pkg.installsize, pkg.remote_location(schemes=["https"])])

        return pkgs

    @method(name="ListFiles")
    def ListFiles(self, package: 's', distro: 's') -> 'as':
        if distro not in self._dnf_objects.keys():
            return ["Invalid distro."]
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()

        available_pkgs.filterm(name=package, arch=["noarch","x86_64"])

        if available_pkgs[0] is None:
            return ["Package {} not found.".format(package)]

        return available_pkgs[0].files

    @method(name="QueryRepoPackage")
    def QueryRepoPackage(self, package: 's', query_type: 's', distro: 's') -> 'as':
        if distro not in self._dnf_objects.keys():
            return ["Invalid distro {}.".format(distro)]
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()

        available_pkgs.filterm(name=package, arch=["noarch","x86_64"])

        if available_pkgs[0] is None:
            return ["Package {} not found.".format(package)]

        if query_type == "provides":
            return [str(reldep) for reldep in available_pkgs[0].provides]
        if query_type == "requires":
            return [str(reldep) for reldep in available_pkgs[0].requires]
        if query_type == "recommends":
            return [str(reldep) for reldep in available_pkgs[0].recommends]
        if query_type == "suggests":
            return [str(reldep) for reldep in available_pkgs[0].suggests]
        if query_type == "supplements":
            return [str(reldep) for reldep in available_pkgs[0].supplements]
        if query_type == "enhances":
            return [str(reldep) for reldep in available_pkgs[0].conflicts]
        if query_type == "obsoletes":
            return [str(reldep) for reldep in available_pkgs[0].obsoletes]

        return ["Invalid query."]

    @method(name="QueryRepo")
    def QueryRepo(self, queries: 'a{ss}', distro: 's') -> 'a(sssiis)':
        if distro not in self._dnf_objects.keys():
            return [['Invalid Distro', 'This is an invalid distro.','N/A', -1, -1, 'N/A']]
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()

        available_pkgs.filterm(arch=["noarch","x86_64"])

        if "file" in queries.keys():
            available_pkgs.filterm(file__glob=queries["file"])
        if "whatconflicts" in queries.keys():
            available_pkgs.filterm(conflicts=queries["whatconflicts"])
        if "whatrequires" in queries.keys():
            available_pkgs.filterm(requires=queries["whatrequires"])
        if "whatobsoletes" in queries.keys():
            available_pkgs.filterm(obsoletes=queries["whatobsoletes"])
        if "whatprovides" in queries.keys():
            q_f_p = available_pkgs.filterm(provides__glob=queries["whatprovides"])
            if q_f_p:
                available_pkgs = q_f_p
            else:
                available_pkgs.filterm(file__glob=queries["whatprovides"])
        if "whatrecommends" in queries.keys():
            available_pkgs.filterm(recommends__glob=queries["whatrecommends"])
        if "whatenhances" in queries.keys():
            available_pkgs.filterm(enhanced__glob=queries["whatenhances"])
        if "whatsupplements" in queries.keys():
            available_pkgs.filterm(supplements__glob=queries["whatsupplements"])
        if "whatsuggests" in queries.keys():
            available_pkgs.filterm(suggests__glob=queries["whatsuggests"])

        pkgs = []
        for pkg in available_pkgs:
            pkgs.append([pkg.name, pkg.summary, pkg.version, pkg.downloadsize, pkg.installsize, pkg.remote_location(schemes=["https"])])

        return pkgs

    @method(name="GetDistros")
    def GetDistros(self) -> 'as':
        keys = []
        for key in self._dnf_objects:
            keys.append(key)
        return keys

    async def RefreshPackages(self):
        while True:
            for key in self._dnf_objects:
                print("Refreshing {}...".format(key))
                await self.RefreshWorker(key)
                print("Refreshed {}!".format(key))
            await asyncio.sleep(1800)

    async def RefreshWorker(self, key):
        try:
            self._dnf_objects[key].reset(goal=True,repos=True,sack=True)
            self._dnf_objects[key].read_all_repos()
            self._dnf_objects[key].fill_sack(load_system_repo=False)
        except:
            return

    def __init__(self, name):
        super().__init__(name)
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
                    self._dnf_objects[key].conf.substitutions['releasever'] = '31'
                if key == "openmandriva":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '4.0'
                if key == "mageia":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '7'
                if key == "centos":
                    self._dnf_objects[key].conf.substitutions['relasever'] = '8-stream'
                self._dnf_objects[key].conf.zchunk = False
                self._dnf_objects[key].conf.reposdir = join(dirname(__file__), key)
                self._dnf_objects[key].conf.cachedir = join(dirname(__file__), "cache", key)
                self._dnf_objects[key].read_all_repos()
                self._dnf_objects[key].fill_sack(load_system_repo=False)
            except Exception as e:
                print("Failed to load {}!".format(key))
                print("Error:\n>>>\t{}".format(e))
                to_pop.append(key)
                continue
            print("Loaded {}!".format(key))
        
        for i in to_pop:
            self._dnf_objects.pop(i)

        print("Repos loaded!")

async def main():
    bus: MessageBus = await MessageBus().connect()
    await bus.request_name("com.github.Appadeia.QueryKit")
    interface = QueryKit("com.github.Appadeia.QueryKit")
    bus.export("/com/github/Appadeia/QueryKit", interface)
    asyncio.ensure_future(interface.RefreshPackages())
    await asyncio.get_event_loop().create_future()

asyncio.get_event_loop().run_until_complete(main())
