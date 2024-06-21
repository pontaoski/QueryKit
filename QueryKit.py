#!/usr/bin/env python3

from os import getcwd, uname
from os.path import join, dirname
from typing import Dict, List

import dnf
import dnf.base
import dnf.conf
import dnf.const
import dnf.query

import hawkey

from dbus_next import BusType
from dbus_next.service import ServiceInterface, method, dbus_property, signal, Variant
from dbus_next.aio import MessageBus

import asyncio
# import pyalpm
import re

from dataclasses import dataclass

@dataclass
class Package():
    name: str
    summary: str
    version: str
    downloadsize: int
    installsize: int
    url: str

class Backend():
    def search_packages(self, query, distro) -> List[Package]:
        return [Package("Not implemented for this backend", "", "", -1, -1, "")]
    def list_files(self, package, distro) -> List[str]:
        return ["Not implemented for this backend"]
    def query_package(self, package, query_type, distro) -> List[str]:
        return ["Not implemented for this backend"]
    def query_repo(self, queries, distro) -> List[Package]:
        return [Package("Not implemented for this backend", "", "", -1, -1, "")]
    def distros(self) -> List[str]:
        return [""]
    def refresh(self): pass
    def init(self): pass

class DnfBackend(Backend):
    _dnf_objects: Dict[str,dnf.Base] = {
        "fedora": dnf.Base(), 
        "tumbleweed": dnf.Base(),
        "leap": dnf.Base(),
        "openmandriva": dnf.Base(),
        "mageia": dnf.Base(),
        "centos": dnf.Base(),
        "packman-leap": dnf.Base(),
        "packman-tumbleweed": dnf.Base(),
        "rpmfusion": dnf.Base()
    }

    def search_packages(self, query, distro) -> List[Package]:
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()
        available_pkgs: dnf.query.Query = available_pkgs.filter(name__substr=query,arch=["noarch","x86_64"])
        pkgs: List[Package] = []
        for pkg in available_pkgs:
            pkgs.append(
                Package(
                    pkg.name or "",
                    pkg.summary or "",
                    pkg.version or "",
                    pkg.downloadsize or -1,
                    pkg.installsize or -1,
                    pkg.remote_location(schemes=["https"]) if pkg.remote_location(schemes=["https"]) is not None else ""
                )
            )
        return pkgs

    def list_files(self, package, distro) -> List[str]:
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()

        available_pkgs.filterm(name=package, arch=["noarch","x86_64"])

        if available_pkgs[0] is None:
            return ["Package {} not found.".format(package)]

        return available_pkgs[0].files

    def query_package(self, package, query_type, distro) -> List[str]:
        dnf_query_obj: dnf.query.Query = self._dnf_objects[distro].sack.query()
        available_pkgs: dnf.query.Query = dnf_query_obj.available()

        available_pkgs.filterm(name=package, arch=["noarch","x86_64"])

        if available_pkgs[0] is None:
            return ["Package {} not found.".format(package)]

        if query_type == "provides":
            return map(str, available_pkgs[0].requires)
        if query_type == "requires":
            return map(str, available_pkgs[0].requires)
        if query_type == "recommends":
            return map(str, available_pkgs[0].recommends)
        if query_type == "suggests":
            return map(str, available_pkgs[0].suggests)
        if query_type == "supplements":
            return map(str, available_pkgs[0].supplements)
        if query_type == "enhances":
            return map(str, available_pkgs[0].conflicts)
        if query_type == "obsoletes":
            return map(str, available_pkgs[0].obsoletes)

        return ["Invalid query."]

    def query_repo(self, queries, distro) -> List[Package]:
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

        return available_pkgs

    def refresh(self):
        for key in self._dnf_objects.keys():
            self._dnf_objects[key].reset(goal=True,repos=True,sack=True)
            self._dnf_objects[key].read_all_repos()
            self._dnf_objects[key].fill_sack(load_system_repo=False)

    def init(self):
        print("Loading dnf repos...")

        arch = hawkey.detect_arch()

        to_pop = []
        for key in self._dnf_objects:
            print(f"Loading {key}...")
            try:
                self._dnf_objects[key].conf.gpgcheck = False
                self._dnf_objects[key].conf.substitutions['arch'] = arch
                self._dnf_objects[key].conf.substitutions['basearch'] = dnf.rpm.basearch(arch)
                if key == "fedora" or key == "rpmfusion":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '40'
                if key == "openmandriva":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '4.1'
                if key == "mageia":
                    self._dnf_objects[key].conf.substitutions['releasever'] = '7'
                self._dnf_objects[key].conf.zchunk = False
                self._dnf_objects[key].conf.reposdir = join(dirname(__file__), key)
                self._dnf_objects[key].conf.cachedir = join(dirname(__file__), "cache", key)
                self._dnf_objects[key].read_all_repos()
                self._dnf_objects[key].fill_sack(load_system_repo=False)
            except Exception as e:
                print(f"Failed to load {key}!")
                print(f"Error:\n>>>\t{e}")
                to_pop.append(key)
                continue
            print(f"Loaded {key}!")
        
        for i in to_pop:
            self._dnf_objects.pop(i)

        print("Dnf repos loaded!")

    def distros(self):
        keys = []
        for key in self._dnf_objects.keys():
            keys.append(key)
        return keys

#class AlpmBackend(Backend):
#    _handles: Dict[str, pyalpm.Handle] = {
#        "arch": pyalpm.Handle(join(getcwd(), "cache/arch"), join(getcwd(), "cache/arch/dbs")),
#        "manjaro": pyalpm.Handle(join(getcwd(), "cache/manjaro"), join(getcwd(), "cache/manjaro/dbs"))
#    }
#
#    _mirrors: Dict[str,List[str]] = {
#        "arch": ["http://mirrors.acm.wpi.edu/archlinux/{repo}/os/{arch}"],
#        "manjaro": ["http://mirror.dacentec.com/manjaro/stable/{repo}/{arch}"]
#    }
#
#    _dbs: Dict[str,List[str]] = {
#        "arch": ["core", "community", "extra"],
#        "manjaro": ["core", "community", "extra", "multilib"],
#    }
#
#    def search_packages(self, query: str, distro: str):
#        handle = self._handles[distro]
#        pkgs = []
#        for db in handle.get_syncdbs():
#            for pkg in db.pkgcache:
#                if query in pkg.name:
#                    pkgs.append(Package(pkg.name, pkg.desc, pkg.version, pkg.size, pkg.isize, pkg.url))
#        return pkgs
#
#    def list_files(self, package: str, distro: str):
#        handle = self._handles[distro]
#        pkg = None
#
#        for db in handle.get_syncdbs():
#            pkg = db.get_pkg(package)
#            if pkg is not None: break
#
#        if pkg is None:
#            return ["Package {} not found.".format(package)]
#
#        print(pkg.files)
#        return pkg.files
#
#    def query_repo(self, queries, distro):
#        handle = self._handles[distro]
#        pkgs = []
#        for db in handle.get_syncdbs():
#            pkgs.extend(db.pkgcache)
#
#        def flatten(pkgs) -> List[str]:
#            ret = []
#            for pkg in pkgs:
#                ret.append(pkg.name)
#            return ret
#        
#        if "whatconflicts" in queries.keys():
#            pkgs = filter(lambda pkg: queries["whatconflicts"] in flatten(pkgs.conflicts), pkgs)
#        if "whatrequires" in queries.keys():
#            pkgs = filter(lambda pkg: queries["whatdepends"] in flatten(pkgs.depends), pkgs)
#        if "whatobsoletes" in queries.keys():
#            pkgs = filter(lambda pkg: queries["whatobsoletes"] in flatten(pkgs.replaces), pkgs)
#        if "whatprovides" in queries.keys():
#            pkgs = filter(lambda pkg: queries["whatprovides"] in flatten(pkgs.provides), pkgs)
#        if "whatrecommends" in queries.keys():
#            pkgs = filter(lambda pkg: queries["whatrecommends"] in flatten(pkgs.optdepends), pkgs)
#
#        return [Package(pkg.name, pkg.desc, pkg.version, pkg.size, pkg.isize, pkg.url) for pkg in pkgs]
#
#    def init(self):
#        print("Loading alpm handles...")
#        for key in self._handles.keys():
#            self._handles[key].arch = uname()[-1]
#            for db in self._dbs[key]:
#                syncdb = self._handles[key].register_syncdb(db, 0)
#                syncdb.servers = [item.format(repo = db, arch = uname()[-1]) for item in self._mirrors[key]]
#                syncdb.update(False)
#        print("Done loading alpm handles!")
#
#    def distros(self):
#        keys = []
#        for key in self._handles.keys():
#            keys.append(key)
#        return keys

class QueryKit(ServiceInterface):
    _backends: List[Backend] = [DnfBackend()]

    def _grabBackendForDistro(self, distro) -> Backend:
        for backend in self._backends:
            for dist in backend.distros():
                if dist == distro:
                    return backend
        return None

    @method(name="SearchPackages")
    def SearchPackages(self, query: 's', distro: 's') -> 'a(sssiis)':
        backend = self._grabBackendForDistro(distro)
        if backend is None:
            return [['Invalid Distro', 'This is an invalid distro.','N/A', -1, -1, 'N/A']]

        pkgs = backend.search_packages(query, distro)
        ret = []
        for pkg in pkgs:
            ret.append([pkg.name, pkg.summary, pkg.version, pkg.downloadsize, pkg.installsize, pkg.url])
        return ret

    @method(name="ListFiles")
    def ListFiles(self, package: 's', distro: 's') -> 'as':
        backend = self._grabBackendForDistro(distro)
        if backend is None:
            return ["Invalid distro."]

        return backend.list_files(package, distro)

    @method(name="QueryRepoPackage")
    def QueryRepoPackage(self, package: 's', query_type: 's', distro: 's') -> 'as':
        backend = self._grabBackendForDistro(distro)
        if backend is None:
            return ["Invalid distro."]

        return backend.query_package(package, query_type, distro)


    @method(name="QueryRepo")
    def QueryRepo(self, queries: 'a{ss}', distro: 's') -> 'a(sssiis)':
        backend = self._grabBackendForDistro(distro)
        if backend is None:
            return [['Invalid Distro', 'This is an invalid distro.','N/A', -1, -1, 'N/A']]

        pkgs = backend.search_packages(queries, distro)
        ret = []
        for pkg in pkgs:
            ret.append([pkg.name, pkg.summary, pkg.version, pkg.downloadsize, pkg.installsize, pkg.url])

        return ret

    @method(name="GetDistros")
    def GetDistros(self) -> 'as':
        return [d for backend in self._backends for d in backend.distros()]

    async def RefreshPackages(self):
        while True:
            for backend in self._backends:
                await self.RefreshWorker(backend)
            await asyncio.sleep(86400)

    async def RefreshWorker(self, backend: Backend):
        try:
            backend.refresh()
        except:
            return

    def __init__(self, name):
        super().__init__(name)
        for backend in self._backends:
            backend.init()

async def main():
    bus: MessageBus = await MessageBus(bus_type = BusType.SYSTEM).connect()
    await bus.request_name("com.github.Appadeia.QueryKit")
    interface = QueryKit("com.github.Appadeia.QueryKit")
    bus.export("/com/github/Appadeia/QueryKit", interface)
    asyncio.ensure_future(interface.RefreshPackages())
    await asyncio.get_event_loop().create_future()

asyncio.get_event_loop().run_until_complete(main())
