# Harklight release installation

The `Harklight release installation` workflow installs only an exact, stable,
published `vX.Y.Z` release from this repository. It runs for the `published`
release event and supports a fresh `workflow_dispatch` at an existing tag for
recovery after an older workflow run can no longer be rerun. The dispatch has
no version, branch, or URL inputs; select the exact tag ref with:

```sh
gh workflow run harklight-release-install.yml \
  --repo clickety-clacks/harklight-server \
  --ref vX.Y.Z
```

The workflow must be present on `main` and in the selected release commit. A
dispatch at a branch, draft, prerelease, unpublished tag, or tag whose workflow
commit differs from its resolved release commit stops before installation. The
workflow resolves the tag through GitHub's tag API, verifies the release tuple
and the exact release assets, then checks the archive digest, `BUILD_INFO`
version/source commit, and packaged `SELF_HOST.md` before invoking the
installer contained in that verified package. It does not check out source on
the server host or execute an unpublished helper.

The installation job requires one repository-scoped self-hosted Linux x86_64
runner carrying the `harklight-release-install` label. Install the reviewed GitHub
Actions runner under the existing Harklight service account in
`~/.local/share/harklight-ci-runner`, register it only to this repository,
and give it only the `harklight-release-install` custom label. Keep its registration
token out of the repository, release assets, workflow output, and logs. The
runner unit template is `deploy/harklight-ci-runner.service`; it uses the
supported `bin/runsvc.sh` entrypoint and runs independently of an interactive
session. Runner registration and activation are performed by the deployment
operator after source review.

The runner host needs Python 3, GNU tar/coreutils, `flock`, and the systemd user
manager already used by the Harklight service. Configure the runner once from
its private directory with the repository URL, an ephemeral registration token
held only in the operator's environment, a stable runner name, and the custom
label; the runner's default `self-hosted`, `linux`, and `x64` labels remain
available to the workflow.

```sh
./config.sh \
  --url https://github.com/clickety-clacks/harklight-server \
  --token "$RUNNER_REGISTRATION_TOKEN" \
  --name harklight-release \
  --labels harklight-release-install \
  --work _work \
  --unattended
```

Copy the service template to
`~/.config/systemd/user/harklight-ci-runner.service`, then run
`systemctl --user daemon-reload`. After repository-scoped registration, the operator may enable
and start the runner with `systemctl --user enable --now
harklight-ci-runner.service`. Check it with
`systemctl --user status harklight-ci-runner.service`; restart it with
`systemctl --user restart harklight-ci-runner.service`, or pause it with
`systemctl --user stop harklight-ci-runner.service`.

To remove the repository runner, disable and stop only
`harklight-ci-runner.service`, then remove the repository registration and
this runner unit:

```sh
systemctl --user disable --now harklight-ci-runner.service
cd ~/.local/share/harklight-ci-runner
./config.sh remove --token "$RUNNER_REMOVAL_TOKEN"
rm ~/.config/systemd/user/harklight-ci-runner.service
systemctl --user daemon-reload
```

Use a short-lived repository runner-removal token. This leaves Harklight state
and `harklight-server.service` in place.

The installation uses the existing root
`~/.local/share/harklight-server`, versioned `releases/<tag>` packages, the
`current` symlink, external `state/relay.sqlite3`, private
`state/server.env`, `bin/start-server`, and the existing user service
`harklight-server.service`. It does not bootstrap, rewrite private config,
replace the data store, rotate identities, or change the existing server or
owner-tunnel service. The job uses repository read access only.

Before an upgrade, it stages and verifies the package, stops the server service
and confirms systemd has no main process (including a pending restart), makes a
stopped private-root backup, then switches `current`. The archive contains the
mutable deployment root, including `state/`, APNs, dashboard TLS, sender, and
other private identity/configuration files. It excludes immutable versioned
releases, prior backups, and the installer lock; those release packages remain
in place, and the `current` symlink is preserved in the archive. Its directory
and archive have owner-only access.
Pending or in-flight provider work remains in the same durable store and follows
the server's existing claim, retry, and expiry behavior after startup. The
installer does not inspect, clear, or mark that work delivered. A provider may
accept a push before the server commits its local completion; restart cannot
establish exactly-once delivery. A repeated tuple that is already installed
and running is a no-op only after the package process matches the selected
release and a read-only authenticated owner `agent-list` request succeeds over
the existing TLS listener. That request is served by the normal Relay/Store
runtime; it creates no message and its response is discarded. If the selected
tuple is already current but its service is stopped, retry cancels any pending
restart, starts that same package, and waits for the same readiness readback.
Installations are serialized by a per-root lock.
Failure retains current data, the backup, and both release identities; recovery
never restores a stale database or automatically switches to an older package.
The actual package and service result, not the workflow's exit alone, determine
whether installation succeeded.

The workflow reads back the selected package process and a successful
authenticated owner TLS request, which establishes that the server application
and its existing store can answer. It does not verify the external push
notification ingress or native notification display; a separate observation
step owns those checks and reports an ingress-only failure as
degraded availability.

This workflow is not a test runner. It has no branch, pull-request, schedule,
or tag-created installation trigger, and it does not run checkout tests or
change anything outside this server installation. Initial installation requires
a published release that contains this workflow and its packaged installation
support.
