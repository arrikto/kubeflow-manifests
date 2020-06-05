#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This file is part of Rok.
#
# Copyright © 2020 Arrikto Inc.  All Rights Reserved.

"""Kubeflow Image Builder.

GCP Reaper stops/deletes resources from a GCP project based on some policy.
Currently, it supports GCE instances and GKE clusters.
"""

import os
import abc
import sys
import docker
import logging
import tempfile
from hashlib import sha256
from ruamel.yaml import YAML

from rok_common import rok_args
from rok_common.cmdutils import yld, run

from rok_tasks import frontend
from rok_tasks.frontend import cli


log = logging.getLogger(__name__)
fr = frontend.get_frontend()

yaml = YAML()

DESCRIPTION = """"Kubeflow Image Builder.

Build images for Kubeflow Applications."""


###############################################################################
# Arguments
#

class ApplicationsFileArgument(rok_args.RokArgument):
    """Option for specifying a file for the images."""

    name = ["-f", "--app-file"]
    dest = "app_file"
    required = True
    help = "File including application image settings."


###############################################################################
# CLI
#

class KubeflowImageBuilderCLI(cli.RokCLI):
    """Kubeflow Image Builder script."""

    DESCRIPTION = DESCRIPTION
    DEFAULT_FRONTEND = "readline"
    DEFAULT_LOGFILE = "kubeflow_image_builder.log"

    def __init__(self):
        self.client = docker.from_env()
        super(KubeflowImageBuilderCLI, self).__init__()

    def initialize_parser(self):
        parser = super(KubeflowImageBuilderCLI, self).initialize_parser()

        parser.add_rok_argument_list([
            ApplicationsFileArgument(),
        ])

        return parser

    def main(self):
        with open(self.args.app_file) as f:
            config = yaml.load(f)
        # Iterate through versions
        for version in config["versions"]:
            name = version["name"]

            # Load repos
            repos = {}
            for repo in version["repos"]:
                r = Repo.from_dict(repo)
                repos[r.name] = r
            manifests_repo = repos["manifests"]

            log.info("Building images for version %s", name)

            # Iterate through applications

            saved_apps = []
            for app_dict in config["applications"]:
                app = Application.from_dict(app_dict, repos)

                msg = "Handling application %s" % app.name
                with fr.Progress(val=0, maxval=3, title="Image Builder", msg=msg) as p:
                    p.info("Building image...")
                    try:
                        app.build_image()
                    except docker.errors.BuildError as e:
                        log.error("Error occured during build %s: %s",
                                  list(map(list, e.build_log)), e.msg)
                        continue
                    p.inc()

                    p.info("Pushing image...")
                    app.push_image()
                    p.inc()

                    p.info("Editing manifests...")
                    app.set_kustomization_image(manifests_repo)
                    saved_apps.append(app)
                    p.inc()

                    p.success()
            create_commit(saved_apps, manifests_repo)


class Application(object):

    def __init__(self, name, docker_context, dockerfile, manifests_dirs,
                 src_img, dst_img, repo):
        self.name = name
        self.client = docker.from_env()
        self.docker_context = docker_context
        self.dockerfile = dockerfile
        self.manifests_dirs = manifests_dirs
        self.src_img = src_img
        self.dst_img = dst_img
        self.repo = repo
        super().__init__()

    @staticmethod
    def from_dict(app, repos):
        params = {p["name"]: p["value"] for p in app["params"]}
        return Application(
            app["name"], params["path_to_context"],
            params["path_to_docker_file"], params["path_to_manifests_dirs"],
            params["src_image_url"], params["dst_image_url"],
            repos[app["sourceRepo"]]
        )

    def build_image(self):
        log.info("Building image for app %s", self.name)
        dockerfile = os.path.join(self.repo.path, self.dockerfile)
        tag = image_str(self.dst_img, self.tag)
        path = os.path.join(self.repo.path, self.docker_context)

        # TODO: The kubeflowversion build argument is used by the Central
        # Dashboard. Either standardize all Dockerfiles to use this build
        # arg, or offer an option to specify a build-arg in the configuration
        # file.
        cmd = ["docker", "build", "-f", dockerfile, "-t", tag,
               "--build-arg", "kubeflowversion=%s" % self.repo.git_hash,
               "--label", "com.arrikto.version=%s" % self.repo.git_hash,
               path]
        env = os.environ.copy()
        env["DOCKER_BUILDKIT"] = "1"
        run(cmd, stdout=sys.stdout, stderr=sys.stderr, log_error=False,
            env=env)

    def push_image(self):
        log.info("Pushing image for app %s", self.name)
        self.client.images.push(self.dst_img, tag=self.tag)

    def set_kustomization_image(self, manifests_repo):
        # Change image in kustomization
        for _dir in self.manifests_dirs:
            kust_path = os.path.join(manifests_repo.path,
                                     _dir, "kustomization.yaml")
            with open(kust_path) as f:
                kustomization = yaml.load(f)
            kustomization = edit_kustomization_image(
                kustomization,
                self.src_img,
                self.dst_img,
                self.repo.git_hash
            )
            with open(kust_path, "w") as f:
                yaml.dump(kustomization, f)

    @property
    def tag(self):
        return self.repo.git_hash

    @property
    def digest(self):
        return self.client.images.get_registry_data(
            image_str(self.dst_img, self.tag)
        ).id


class Repo(object):

    __metaclass__ = abc.ABCMeta

    @staticmethod
    def from_dict(repo):
        t = repo["resourceSpec"]["type"]
        name = repo["name"]
        if t == "local":
            params = {p["name"]: p["value"] for p in repo["resourceSpec"]["params"]}
            return LocalRepo(name, params["path"])
        else:
            raise NotImplementedError("Only local repos supported.")

    @property
    def git_hash(self):
        """Return the git hash of the repo."""
        git_version = yld(["git", "describe", "--tags", "--long", "--always"],
                          cwd=self.path)
        changed_files = yld(["git", "diff-files"], cwd=self.path).strip()
        if changed_files:
            suffix = sha256(changed_files.encode("utf-8")).hexdigest()[:8]
            return git_version + "-dirty-" + suffix
        return git_version

    @property
    def path(self):
        """Path of the repo in the local filesystem."""
        return self._path

    def __hash__(self):
        return hash(self.name)


class LocalRepo(Repo):

    def __init__(self, name, path):
        # TODO: git checkout the revision
        self.name = name
        self._path = path
        super(LocalRepo, self).__init__()


###############################################################################
# Utils
#

def create_commit(apps, manifests_repo):
    msg = "Update images\n\n"
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(msg)
        for app in apps:
            for _dir in app.manifests_dirs:
                kust = os.path.join(manifests_repo.path, _dir,
                                    "kustomization.yaml")
                yld(["git", "add", kust], cwd=manifests_repo.path)
                msg = ("Updated app '%s' image to %s, in kustomization %s\n" %
                       (app.name, app.tag, _dir))
                f.write(msg)
        f.flush()
        yld(["git", "commit", "-F", f.name], cwd=manifests_repo.path)


def image_str(img, tag, digest=""):
    img = "%s:%s" % (img, tag)
    if digest:
        img += "@%s" % digest
    return img


def edit_kustomization_image(kustomization, src_img, new_name, new_tag,
                             new_digest=None):

    def _set(d, key, val):
        if val:
            d[key] = val

    def _amend_image(img={}, new_name=new_name, new_tag=new_tag,
                     new_digest=new_digest):
        _set(img, "newName", new_name)
        if new_digest:
            new_tag += "@%s" % new_digest
        _set(img, "newTag", new_tag)

    if "images" not in kustomization:
        kustomization["images"] = []
    for image in kustomization["images"]:
        if image["name"] == src_img:
            # Amend the object to retain comments
            _amend_image(image)
            break
    else:
        kustomization["images"].append({"name": src_img})
        _amend_image(kustomization["images"][-1])

    return kustomization


def main():
    _cli = KubeflowImageBuilderCLI()
    return _cli.run()


if __name__ == "__main__":
    sys.exit(main())
