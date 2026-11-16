#!/usr/bin/env python3
"""Validate and read back Harklight Server release packages for tag publishing."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


REPOSITORY = "clickety-clacks/harklight-server"
BOT_LOGIN = "github-actions[bot]"


def fail(message: str) -> None:
    raise SystemExit(message)


def tag_version(tag: str) -> str:
    match = re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag)
    if not match:
        fail(f"tag is not an exact stable version tag: {tag}")
    return tag[1:]


def api_base() -> str:
    return os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")


def api_url(path: str) -> str:
    return f"{api_base()}/repos/{REPOSITORY}/{path.lstrip('/')}"


def request_json(url: str, token: str | None = None, allow_404: bool = False):
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "HarklightReleaseWorkflow",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if allow_404 and error.code == 404:
            return None
        fail(f"GitHub API request failed with HTTP {error.code}")
    except (urllib.error.URLError, OSError, ValueError):
        fail("GitHub API request failed")


def write_outputs(values: dict[str, str]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with open(output_path, "a", encoding="utf-8") as output:
        for name, value in values.items():
            if "\n" in value or "\r" in value:
                fail(f"workflow output contains a newline: {name}")
            output.write(f"{name}={value}\n")


def source_value(path: Path, pattern: str, label: str) -> str:
    matches = re.findall(pattern, path.read_text(encoding="utf-8"), re.MULTILINE)
    if len(matches) != 1:
        fail(f"could not read one {label} from {path}")
    return matches[0]


def cmd_toolchain(_args) -> None:
    server_ci = Path(".github/workflows/server-ci.yml")
    otp = source_value(server_ci, r"^\s+otp-version:\s*['\"]?([^'\"\s]+)", "OTP version")
    elixir = source_value(server_ci, r"^\s+elixir-version:\s*['\"]?([^'\"\s]+)", "Elixir version")
    write_outputs({"otp": otp, "elixir": elixir})
    print(f"Using Server CI pins: OTP {otp}, Elixir {elixir}")


def cmd_validate_source(args) -> None:
    version = tag_version(args.tag)
    if os.environ.get("GITHUB_REPOSITORY", REPOSITORY) != REPOSITORY:
        fail("workflow ran outside the Harklight Server repository")
    if os.environ.get("GITHUB_EVENT_NAME", "push") != "push":
        fail("release publishing requires a tag push")
    if os.environ.get("GITHUB_REF_TYPE", "tag") != "tag":
        fail("release publishing requires a tag ref")
    expected_ref = f"refs/tags/{args.tag}"
    if os.environ.get("GITHUB_REF", expected_ref) != expected_ref:
        fail("workflow ref does not match the selected tag")

    commit = args.commit.lower()
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        fail("tag commit identity is invalid")
    head = subprocess.check_output(["git", "rev-parse", "HEAD^{commit}"], text=True).strip().lower()
    if head != commit:
        fail("checked-out source differs from the tagged commit")
    event_commit = os.environ.get("GITHUB_SHA", commit).lower()
    if event_commit != commit:
        fail("event commit differs from the tagged commit")

    status = subprocess.check_output(["git", "status", "--porcelain"], text=True)
    if status:
        fail("tagged source checkout is not clean")
    fetch = subprocess.run(
        ["git", "fetch", "--no-tags", "origin", "+refs/heads/main:refs/remotes/origin/main"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if fetch.returncode != 0:
        fail("could not fetch main for tag ancestry validation")
    ancestry = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "refs/remotes/origin/main"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if ancestry.returncode == 1:
        fail("tag commit is not reachable from main")
    if ancestry.returncode != 0:
        fail("could not evaluate whether the tag commit is on main")

    mix_version = source_value(
        Path("mix.exs"), r'^\s*version:\s*"([^"\n]+)"\s*,?\s*$', "mix.exs version"
    )
    package_version = source_value(
        Path("scripts/build-package.sh"), r"^version=([0-9]+\.[0-9]+\.[0-9]+)$", "package version"
    )
    if mix_version != version:
        fail(f"tag version {version} differs from mix.exs version {mix_version}")
    if package_version != version:
        fail(f"tag version {version} differs from scripts/build-package.sh version {package_version}")
    archive_name = f"harklight-server-{version}-linux-x86_64.tar.gz"
    if archive_name not in Path("SELF_HOST.md").read_text(encoding="utf-8"):
        fail(f"SELF_HOST.md does not name the tag archive {archive_name}")

    write_outputs({"tag": args.tag, "version": version, "commit": commit})
    print(f"Validated {args.tag} at {commit} on main with a clean source tree")


def release_for_tag(tag: str, token: str | None = None, allow_404: bool = False):
    encoded = urllib.parse.quote(tag, safe="")
    return request_json(api_url(f"releases/tags/{encoded}"), token=token, allow_404=allow_404)


def cmd_classify(args) -> None:
    token = os.environ.get("GH_TOKEN")
    if not token:
        fail("repository release read token is unavailable")
    release = release_for_tag(args.tag, token=token, allow_404=True)
    if release is None:
        write_outputs({"mode": "build", "release_id": ""})
        print("No release exists for this tag; the package will be built")
        return
    if release.get("tag_name") != args.tag:
        fail("release metadata names a different tag")
    if release.get("draft") is False and release.get("prerelease") is False and release.get("published_at"):
        release_id = release.get("id")
        if not isinstance(release_id, int) or release_id <= 0:
            fail("published release identity is invalid")
        write_outputs({"mode": "readback", "release_id": str(release_id)})
        print("A stable published release exists; it will be checked without rebuilding")
        return
    if release.get("draft") is True:
        author = release.get("author") or {}
        if author.get("login") != BOT_LOGIN:
            fail("a draft release for this tag was not created by the release workflow; leaving it unchanged")
        release_id = release.get("id")
        if not isinstance(release_id, int) or release_id <= 0:
            fail("release draft identity is invalid")
        write_outputs({"mode": "build", "release_id": str(release_id)})
        print("A prior workflow draft may be replaced after the new package verifies")
        return
    fail("an existing prerelease or incomplete release cannot be changed by this workflow")


def list_release_assets(release_id: int, token: str | None = None) -> list[dict]:
    assets: list[dict] = []
    page = 1
    while page <= 10:
        query = urllib.parse.urlencode({"per_page": 100, "page": page})
        response = request_json(api_url(f"releases/{release_id}/assets?{query}"), token=token)
        if not isinstance(response, list):
            fail("release asset listing is unavailable")
        assets.extend(response)
        if len(response) < 100:
            return assets
        page += 1
    fail("release asset listing exceeds the supported page limit")


def required_asset_names(version: str) -> tuple[str, str, str]:
    return (f"harklight-server-{version}-linux-x86_64.tar.gz", "SHA256SUMS", "SELF_HOST.md")


def selected_assets(release_id: int, version: str, token: str | None = None) -> dict[str, dict]:
    expected_names = required_asset_names(version)
    assets = list_release_assets(release_id, token=token)
    selected: dict[str, dict] = {}
    for asset in assets:
        name = asset.get("name")
        if name in expected_names:
            if name in selected:
                fail(f"release contains duplicate required asset: {name}")
            if asset.get("state") != "uploaded" or not isinstance(asset.get("size"), int) or asset["size"] <= 0:
                fail(f"required release asset is not complete: {name}")
            selected[name] = asset
    if set(selected) != set(expected_names):
        fail("release is missing a required Harklight package asset")
    if len(assets) != len(expected_names):
        fail("release does not contain exactly the three required package assets")
    return selected


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_public_asset(tag: str, name: str, destination: Path) -> None:
    if os.environ.get("GITHUB_REPOSITORY", REPOSITORY) != REPOSITORY:
        fail("public asset download ran outside the Harklight Server repository")
    encoded_tag = urllib.parse.quote(tag, safe="")
    encoded_name = urllib.parse.quote(name, safe="")
    url = f"https://github.com/{REPOSITORY}/releases/download/{encoded_tag}/{encoded_name}"
    request = urllib.request.Request(url, headers={"User-Agent": "HarklightReleaseWorkflow"})
    try:
        with urllib.request.urlopen(request, timeout=120) as response, destination.open("wb") as output:
            if response.getcode() != 200:
                fail(f"public release download returned HTTP {response.getcode()}: {name}")
            shutil.copyfileobj(response, output, length=1024 * 1024)
    except urllib.error.HTTPError as error:
        fail(f"public release download failed with HTTP {error.code}: {name}")
    except (urllib.error.URLError, OSError):
        fail(f"public release download failed: {name}")


def read_archive_member(archive: Path, member_name: str) -> bytes:
    try:
        with tarfile.open(archive, "r:gz") as package:
            matches = [member for member in package.getmembers() if member.name == member_name]
            if len(matches) != 1 or not matches[0].isfile():
                fail(f"package member is missing or duplicated: {member_name}")
            stream = package.extractfile(matches[0])
            if stream is None:
                fail(f"package member cannot be read: {member_name}")
            return stream.read()
    except (tarfile.TarError, OSError):
        fail("release package archive cannot be read")


def build_info_values(data: bytes) -> dict[str, str]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        fail("packaged BUILD_INFO is not UTF-8")
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in {"version", "source_commit", "source_status"}:
            if key in values:
                fail(f"packaged BUILD_INFO duplicates {key}")
            values[key] = value
    return values


def build_toolchain_lines(data: bytes) -> list[str]:
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeDecodeError:
        fail("packaged BUILD_INFO is not UTF-8")
    version_lines = [index for index, line in enumerate(lines) if line.startswith("Erlang/OTP ")]
    dependency_markers = [index for index, line in enumerate(lines) if line == "native_dependencies"]
    if len(version_lines) != 1 or len(dependency_markers) != 1 or version_lines[0] >= dependency_markers[0]:
        fail("packaged BUILD_INFO does not contain one complete mix --version record")
    toolchain_lines = lines[version_lines[0] : dependency_markers[0]]
    toolchain_identity(toolchain_lines, "packaged BUILD_INFO")
    return toolchain_lines


def toolchain_identity(lines: list[str], label: str) -> tuple[str, str, str]:
    # Mix may separate its two version lines with an empty line. Preserve all
    # lines in BUILD_INFO and the tag record, but compare only the defined
    # OTP release, ERTS version, and Mix version line.
    nonempty_lines = [line for line in lines if line]
    if len(nonempty_lines) != 2:
        fail(f"{label} does not contain exactly the two expected mix --version lines")
    otp_matches = re.findall(r"^Erlang/OTP\s+(\S+)", nonempty_lines[0])
    erts_matches = re.findall(r"\berts-([^\]\s]+)", nonempty_lines[0])
    mix_line = nonempty_lines[1]
    if (
        len(otp_matches) != 1
        or len(erts_matches) != 1
        or not re.fullmatch(r"Mix\s+\S+\s+\(compiled with Erlang/OTP\s+[^)]+\)", mix_line)
    ):
        fail(f"{label} has malformed mix --version lines")
    return otp_matches[0], erts_matches[0], mix_line


def tag_annotation(tag: str) -> str:
    tag_ref = f"refs/tags/{tag}"
    try:
        object_type = subprocess.check_output(["git", "cat-file", "-t", tag_ref], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        fail("could not read the pushed version tag")
    if object_type != "tag":
        fail("pushed version tag is lightweight; an annotated tag with rehearsal fields is required")
    try:
        return subprocess.check_output(
            ["git", "for-each-ref", "--format=%(contents)", tag_ref], text=True
        )
    except (OSError, subprocess.CalledProcessError):
        fail("could not read the annotated tag message")


def parse_rehearsal_record(message: str) -> tuple[str, dict[str, str], list[str]]:
    fields: dict[str, str] = {}
    toolchain_lines: list[str] = []
    allowed_fields = {
        "rehearsal",
        "rehearsal-archive-sha256",
        "rehearsal-source-commit",
        "rehearsal-build-script-sha256",
        "rehearsal-toolchain",
    }
    for line in message.splitlines():
        if not (line.startswith("rehearsal:") or line.startswith("rehearsal-") or line.startswith("rehearsal ")):
            continue
        key, separator, value = line.partition(":")
        if not separator or key not in allowed_fields or not value.startswith(" "):
            fail("malformed rehearsal record; expected the documented key: value fields")
        value = value[1:]
        if key == "rehearsal-toolchain":
            if not value:
                fail("rehearsal-toolchain fields must contain a version-output line")
            toolchain_lines.append(value)
        else:
            if not value or key in fields:
                fail("malformed rehearsal record; fields must be nonempty and unique")
            fields[key] = value

    expected = {
        "rehearsal-archive-sha256",
        "rehearsal-source-commit",
        "rehearsal-build-script-sha256",
    }
    if fields.get("rehearsal") == "not-required":
        if set(fields) != {"rehearsal"} or toolchain_lines:
            fail("malformed rehearsal record; rehearsal: not-required must be the only rehearsal field")
        return "not-required", fields, toolchain_lines
    if "rehearsal" in fields or set(fields) != expected or not toolchain_lines:
        fail(
            "annotated tag is missing or has a malformed rehearsal record; expected rehearsal: not-required, "
            "or rehearsal-archive-sha256, rehearsal-source-commit, rehearsal-build-script-sha256 and "
            "rehearsal-toolchain fields"
        )
    if not re.fullmatch(r"[0-9a-fA-F]{64}", fields["rehearsal-archive-sha256"]):
        fail("rehearsal-archive-sha256 must be a 64-character SHA-256 digest")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", fields["rehearsal-source-commit"]):
        fail("rehearsal-source-commit must be a full commit SHA")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", fields["rehearsal-build-script-sha256"]):
        fail("rehearsal-build-script-sha256 must be a 64-character SHA-256 digest")
    fields["rehearsal-archive-sha256"] = fields["rehearsal-archive-sha256"].lower()
    fields["rehearsal-source-commit"] = fields["rehearsal-source-commit"].lower()
    fields["rehearsal-build-script-sha256"] = fields["rehearsal-build-script-sha256"].lower()
    return "recorded", fields, toolchain_lines


def cmd_validate_rehearsal(args) -> None:
    version = tag_version(args.tag)
    message = tag_annotation(args.tag)
    form, fields, recorded_toolchain = parse_rehearsal_record(message)
    if form == "not-required":
        write_outputs({"result": "not_required"})
        print("The tag records that FORMAT rehearsal is not required; no package comparison is needed")
        return

    if fields["rehearsal-source-commit"] != args.commit.lower():
        fail("rehearsal-source-commit differs from the tagged commit; the rehearsal does not count")

    dist_dir = Path(args.dist_dir)
    archive = dist_dir / f"harklight-server-{version}-linux-x86_64.tar.gz"
    build_info_path = dist_dir / "BUILD_INFO"
    if not archive.is_file() or not build_info_path.is_file():
        fail("verified CI package or packaged BUILD_INFO is unavailable for rehearsal comparison")
    package_toolchain = build_toolchain_lines(build_info_path.read_bytes())
    toolchain_identity(recorded_toolchain, "annotated tag rehearsal record")

    digest_a = fields["rehearsal-archive-sha256"]
    digest_b = digest_file(archive)
    build_script_digest = digest_file(Path(args.build_script))
    if fields["rehearsal-build-script-sha256"] != build_script_digest:
        write_outputs({"result": "mismatch", "reason": "build-script-mismatch"})
        print("The CI package must be rehearsed before publication because the build script differs")
    elif toolchain_identity(recorded_toolchain, "annotated tag rehearsal record") != toolchain_identity(
        package_toolchain, "packaged BUILD_INFO"
    ):
        write_outputs({"result": "mismatch", "reason": "toolchain-identity-mismatch"})
        print("The CI package must be rehearsed before publication because the toolchain identity differs")
    else:
        write_outputs({"result": "match"})
        print("The rehearsal build script and toolchain identity match the verified CI package")
    print(f"Rehearsal archive SHA-256 (A): {digest_a}")
    print(f"CI package archive SHA-256 (B): {digest_b}")


def verify_package_files(dist_dir: Path, tag: str, commit: str, version: str, guide_path: Path) -> dict[str, str]:
    if tag_version(tag) != version:
        fail("release tag version differs from the expected package version")
    archive_name = f"harklight-server-{version}-linux-x86_64.tar.gz"
    archive = dist_dir / archive_name
    sums = dist_dir / "SHA256SUMS"
    guide = dist_dir / "SELF_HOST.md"
    for path in (archive, sums, guide):
        if not path.is_file() or path.stat().st_size == 0:
            fail(f"release package file is missing or empty: {path.name}")

    try:
        sum_lines = sums.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeDecodeError):
        fail("SHA256SUMS is not readable ASCII")
    if len(sum_lines) != 1:
        fail("SHA256SUMS must contain one archive entry")
    match = re.fullmatch(r"([0-9a-f]{64})  ([^\s]+)", sum_lines[0])
    if not match or match.group(2) != archive_name:
        fail("SHA256SUMS does not name the expected release archive")
    archive_digest = digest_file(archive)
    if match.group(1) != archive_digest:
        fail("release archive SHA-256 does not match SHA256SUMS")

    root = archive_name.removesuffix(".tar.gz")
    build_info = read_archive_member(archive, f"{root}/BUILD_INFO")
    package_guide = read_archive_member(archive, f"{root}/SELF_HOST.md")
    values = build_info_values(build_info)
    if values.get("version") != version:
        fail("packaged BUILD_INFO version differs from the tag")
    if values.get("source_commit", "").lower() != commit.lower():
        fail("packaged BUILD_INFO source commit differs from the tag")
    if values.get("source_status") != "":
        fail("packaged BUILD_INFO records a dirty source tree")
    (dist_dir / "BUILD_INFO").write_bytes(build_info)
    try:
        source_guide = guide_path.read_bytes()
        asset_guide = guide.read_bytes()
    except OSError:
        fail("release guide cannot be read")
    if package_guide != source_guide or asset_guide != source_guide:
        fail("packaged or published SELF_HOST.md differs from the guide at the tagged commit")

    hashes = {
        archive_name: archive_digest,
        "SHA256SUMS": digest_file(sums),
        "SELF_HOST.md": digest_file(guide),
    }
    print(f"Tarball SHA-256: {archive_digest}")
    print("Packaged BUILD_INFO:")
    sys.stdout.write(build_info.decode("utf-8"))
    write_outputs({"hashes": json.dumps(hashes, sort_keys=True, separators=(",", ":"))})
    return hashes


def cmd_verify_package(args) -> None:
    verify_package_files(Path(args.dist_dir), args.tag, args.commit, args.version, Path(args.guide))


def cmd_verify_existing(args) -> None:
    release = release_for_tag(args.tag)
    if (
        not release
        or release.get("id") != args.release_id
        or release.get("tag_name") != args.tag
        or release.get("draft") is not False
        or release.get("prerelease") is not False
        or not release.get("published_at")
    ):
        fail("existing published release changed before its verification")
    assets = selected_assets(args.release_id, args.version)
    dist_dir = Path(args.dist_dir)
    dist_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name in (f"harklight-server-{args.version}-linux-x86_64.tar.gz", "SHA256SUMS", "SELF_HOST.md"):
        asset = assets.get(name)
        if not asset:
            fail(f"published release is missing required asset: {name}")
        destination = dist_dir / name
        download_public_asset(args.tag, name, destination)
        if destination.stat().st_size != asset["size"]:
            fail(f"public download size differs from release metadata: {name}")
    guide_asset = (dist_dir / "SELF_HOST.md").read_bytes()
    if guide_asset != Path(args.guide).read_bytes():
        fail("published SELF_HOST.md differs from the guide at the tagged commit")
    verify_package_files(dist_dir, args.tag, args.commit, args.version, Path(args.guide))
    print("Existing release passed P-7 package checks without being modified")


def cmd_delete_draft(args) -> None:
    token = os.environ.get("GH_TOKEN")
    if not token:
        fail("repository release write token is unavailable")
    release = release_for_tag(args.tag, token=token)
    if (
        not release
        or release.get("id") != args.release_id
        or release.get("tag_name") != args.tag
        or release.get("draft") is not True
        or (release.get("author") or {}).get("login") != BOT_LOGIN
    ):
        fail("prior workflow draft changed; leaving it unchanged")
    if tag_commit_from_public_api(args.tag) != args.commit.lower():
        fail("prior draft tag resolves to a different commit; leaving it unchanged")
    request = urllib.request.Request(
        api_url(f"releases/{args.release_id}"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "HarklightReleaseWorkflow",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.getcode() not in (200, 202, 204):
                fail("prior workflow draft could not be removed")
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        fail("prior workflow draft could not be removed")
    print("Replaced the prior unpublished workflow draft; the pushed tag was left unchanged")


def cmd_assert_draft(args) -> None:
    token = os.environ.get("GH_TOKEN")
    if not token:
        fail("repository release read token is unavailable")
    release = release_for_tag(args.tag, token=token)
    if (
        not release
        or release.get("tag_name") != args.tag
        or release.get("draft") is not True
        or release.get("prerelease") is not False
        or (release.get("author") or {}).get("login") != BOT_LOGIN
    ):
        fail("release is not the expected unpublished draft")
    expected_notes = Path(args.notes_file).read_text(encoding="utf-8")
    if release.get("body") != expected_notes:
        fail("draft release body differs from RELEASE_NOTES.md at the tagged commit")
    if tag_commit_from_public_api(args.tag) != args.commit.lower():
        fail("draft release tag resolves to a different commit")
    release_id = release.get("id")
    if not isinstance(release_id, int) or release_id <= 0:
        fail("draft release identity is invalid")
    assets = selected_assets(release_id, args.version, token=token)
    expected_sizes = {
        f"harklight-server-{args.version}-linux-x86_64.tar.gz": (Path(args.dist_dir) / f"harklight-server-{args.version}-linux-x86_64.tar.gz").stat().st_size,
        "SHA256SUMS": (Path(args.dist_dir) / "SHA256SUMS").stat().st_size,
        "SELF_HOST.md": (Path(args.dist_dir) / "SELF_HOST.md").stat().st_size,
    }
    for name, expected_size in expected_sizes.items():
        if assets[name]["size"] != expected_size:
            fail(f"uploaded draft asset size differs from the verified file: {name}")
    print("All three verified files are uploaded to the draft before publication")


def tag_commit_from_public_api(tag: str) -> str:
    encoded = urllib.parse.quote(tag, safe="")
    reference = request_json(api_url(f"git/ref/tags/{encoded}"))
    obj = reference.get("object") or {}
    seen: set[str] = set()
    for _ in range(16):
        sha = str(obj.get("sha", "")).lower()
        if not re.fullmatch(r"[0-9a-f]{40}", sha) or sha in seen:
            fail("public tag reference cannot be resolved to a commit")
        seen.add(sha)
        if obj.get("type") == "commit":
            return sha
        if obj.get("type") != "tag":
            fail("public tag reference does not resolve to a commit")
        annotated = request_json(api_url(f"git/tags/{sha}"))
        obj = annotated.get("object") or {}
    fail("public annotated tag chain is too deep")


def cmd_public_readback(args) -> None:
    try:
        expected_hashes = json.loads(args.expected_hashes)
    except json.JSONDecodeError:
        fail("verified package digest record is invalid")
    if not isinstance(expected_hashes, dict):
        fail("verified package digest record is invalid")
    encoded = urllib.parse.quote(args.tag, safe="")
    release = request_json(api_url(f"releases/tags/{encoded}"))
    if (
        release.get("tag_name") != args.tag
        or release.get("draft") is not False
        or release.get("prerelease") is not False
        or not release.get("published_at")
    ):
        fail("public release readback is not a published stable release")
    if tag_commit_from_public_api(args.tag) != args.commit.lower():
        fail("public tag resolves to a different commit")
    assets = selected_assets(int(release.get("id", 0)), args.version)

    runner_temp = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir()))
    readback_dir = Path(tempfile.mkdtemp(prefix="harklight-public-readback-", dir=runner_temp))
    try:
        names = (f"harklight-server-{args.version}-linux-x86_64.tar.gz", "SHA256SUMS", "SELF_HOST.md")
        for name in names:
            asset = assets.get(name)
            if not asset:
                fail(f"public release readback is missing asset: {name}")
            destination = readback_dir / name
            download_public_asset(args.tag, name, destination)
            if destination.stat().st_size != asset["size"]:
                fail(f"public release readback size differs from metadata: {name}")
            digest = digest_file(destination)
            if expected_hashes.get(name) != digest:
                fail(f"public release readback digest differs from the verified file: {name}")
            print(f"Public download SHA-256 {name}: {digest}")
    finally:
        shutil.rmtree(readback_dir, ignore_errors=True)
    print(f"Public release readback confirmed {args.tag} at {args.commit}")


def add_package_args(parser: argparse.ArgumentParser, need_dist: bool = True) -> None:
    if need_dist:
        parser.add_argument("--dist-dir", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--guide", default="SELF_HOST.md")


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    toolchain = commands.add_parser("toolchain")
    toolchain.set_defaults(function=cmd_toolchain)

    validate = commands.add_parser("validate-source")
    validate.add_argument("--tag", required=True)
    validate.add_argument("--commit", required=True)
    validate.set_defaults(function=cmd_validate_source)

    classify = commands.add_parser("classify")
    classify.add_argument("--tag", required=True)
    classify.set_defaults(function=cmd_classify)

    package = commands.add_parser("verify-package")
    add_package_args(package)
    package.set_defaults(function=cmd_verify_package)

    rehearsal = commands.add_parser("validate-rehearsal")
    rehearsal.add_argument("--dist-dir", required=True)
    rehearsal.add_argument("--tag", required=True)
    rehearsal.add_argument("--commit", required=True)
    rehearsal.add_argument("--build-script", default="scripts/build-package.sh")
    rehearsal.set_defaults(function=cmd_validate_rehearsal)

    existing = commands.add_parser("verify-existing")
    add_package_args(existing)
    existing.add_argument("--release-id", required=True, type=int)
    existing.set_defaults(function=cmd_verify_existing)

    delete_draft = commands.add_parser("delete-draft")
    delete_draft.add_argument("--tag", required=True)
    delete_draft.add_argument("--commit", required=True)
    delete_draft.add_argument("--release-id", required=True, type=int)
    delete_draft.set_defaults(function=cmd_delete_draft)

    assert_draft = commands.add_parser("assert-draft")
    assert_draft.add_argument("--tag", required=True)
    assert_draft.add_argument("--commit", required=True)
    assert_draft.add_argument("--version", required=True)
    assert_draft.add_argument("--dist-dir", required=True)
    assert_draft.add_argument("--notes-file", default="RELEASE_NOTES.md")
    assert_draft.set_defaults(function=cmd_assert_draft)

    readback = commands.add_parser("public-readback")
    readback.add_argument("--tag", required=True)
    readback.add_argument("--commit", required=True)
    readback.add_argument("--version", required=True)
    readback.add_argument("--expected-hashes", required=True)
    readback.set_defaults(function=cmd_public_readback)

    args = parser.parse_args()
    args.function(args)


if __name__ == "__main__":
    main()
