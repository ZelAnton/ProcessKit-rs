#!/usr/bin/env python3
"""Install and diagnose the external tools used by local CI recipes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import subprocess
import sys


REPOSITORY = Path(__file__).resolve().parent.parent
MDBOOK_VERSION = "0.4.40"
NEXTEST_SERIES = (0, 9)
GIT_CLIFF_MAJOR = 2
MSRV_TARGETS = ("x86_64-pc-windows-msvc", "aarch64-apple-darwin")


@dataclass(frozen=True)
class Tool:
    label: str
    executable: str
    package: str
    exact: str | None = None
    series: tuple[int, ...] | None = None
    install_version: str | None = None


TOOLS = (
    Tool("just", "just", "just"),
    Tool("cargo-hack", "cargo-hack", "cargo-hack"),
    Tool(
        "cargo-nextest",
        "cargo-nextest",
        "cargo-nextest",
        series=NEXTEST_SERIES,
        install_version="^0.9",
    ),
    Tool("typos-cli", "typos", "typos-cli"),
    Tool("cargo-public-api", "cargo-public-api", "cargo-public-api"),
    Tool("cargo-fuzz", "cargo-fuzz", "cargo-fuzz"),
    Tool(
        "mdBook",
        "mdbook",
        "mdbook",
        exact=MDBOOK_VERSION,
        install_version=MDBOOK_VERSION,
    ),
    Tool(
        "git-cliff",
        "git-cliff",
        "git-cliff",
        series=(GIT_CLIFF_MAJOR,),
        install_version=f"^{GIT_CLIFF_MAJOR}",
    ),
)


@dataclass(frozen=True)
class Check:
    label: str
    ok: bool
    detail: str
    required: bool = True


def msrv() -> str:
    manifest = (REPOSITORY / "Cargo.toml").read_text(encoding="utf-8")
    match = re.search(r'^rust-version\s*=\s*"([^"]+)"', manifest, re.MULTILINE)
    if match is None:
        raise RuntimeError("Cargo.toml has no rust-version")
    return match.group(1)


def output(args: list[str]) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            args,
            cwd=REPOSITORY,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, f"timed out: {' '.join(args)}"
    except OSError as error:
        return False, str(error)
    text = " ".join(result.stdout.split())
    return result.returncode == 0, text


def parsed_version(text: str) -> tuple[int, ...] | None:
    match = re.search(r"(?<!\d)(\d+(?:\.\d+)+)", text)
    if match is None:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def tool_check(tool: Tool) -> Check:
    path = shutil.which(tool.executable)
    if path is None:
        return Check(tool.label, False, "not found on PATH")
    if tool.executable.startswith("cargo-"):
        cargo = shutil.which("cargo")
        if cargo is None:
            return Check(tool.label, False, "cargo is not found on PATH")
        version_command = [
            cargo,
            tool.executable.removeprefix("cargo-"),
            "--version",
        ]
    else:
        version_command = [path, "--version"]
    succeeded, version_output = output(version_command)
    if not succeeded:
        return Check(tool.label, False, f"version check failed: {version_output}")
    version = parsed_version(version_output)
    if version is None:
        return Check(tool.label, False, f"unrecognized version: {version_output}")
    dotted = ".".join(str(part) for part in version)
    if tool.exact is not None and dotted != tool.exact:
        return Check(tool.label, False, f"found {dotted}; need exactly {tool.exact}")
    if tool.series is not None and version[: len(tool.series)] != tool.series:
        need = ".".join(str(part) for part in tool.series)
        return Check(tool.label, False, f"found {dotted}; need {need}.x")
    return Check(tool.label, True, version_output)


def rustup_check() -> list[Check]:
    rustup = shutil.which("rustup")
    if rustup is None:
        return [Check("rustup", False, "not found on PATH")]

    checks: list[Check] = []
    succeeded, installed = output(
        [rustup, "component", "list", "--toolchain", "stable", "--installed"]
    )
    component_lines = installed.split() if succeeded else []
    missing_components = [
        component
        for component in ("rustfmt", "clippy")
        if not any(
            line == component or line.startswith(component + "-")
            for line in component_lines
        )
    ]
    checks.append(
        Check(
            "stable components",
            succeeded and not missing_components,
            "rustfmt and clippy installed"
            if succeeded and not missing_components
            else "missing: " + ", ".join(missing_components or [installed]),
        )
    )

    succeeded, nightly = output([rustup, "run", "nightly", "rustc", "--version"])
    checks.append(
        Check(
            "nightly toolchain",
            succeeded,
            nightly if succeeded else "install with `rustup toolchain install nightly --profile minimal`",
        )
    )

    required_msrv = msrv()
    succeeded, compiler = output(
        [rustup, "run", required_msrv, "rustc", "--version"]
    )
    checks.append(
        Check(
            f"MSRV toolchain {required_msrv}",
            succeeded,
            compiler
            if succeeded
            else f"install with `rustup toolchain install {required_msrv} --profile minimal`",
        )
    )
    if succeeded:
        listed, targets_output = output(
            [
                rustup,
                "target",
                "list",
                "--toolchain",
                required_msrv,
                "--installed",
            ]
        )
        installed_targets = set(targets_output.split()) if listed else set()
        missing_targets = sorted(set(MSRV_TARGETS) - installed_targets)
        checks.append(
            Check(
                "MSRV cross-targets",
                listed and not missing_targets,
                "all installed"
                if listed and not missing_targets
                else "missing: " + ", ".join(missing_targets or [targets_output]),
            )
        )
    else:
        checks.append(Check("MSRV cross-targets", False, "MSRV toolchain is unavailable"))
    return checks


def docker_check() -> Check:
    docker = shutil.which("docker")
    if docker is None:
        return Check(
            "Docker/musl",
            False,
            "Docker is not installed; `just test-musl` remains unavailable",
            required=False,
        )
    succeeded, version = output(
        [docker, "version", "--format", "{{.Server.Version}}"]
    )
    return Check(
        "Docker/musl",
        succeeded,
        f"server {version}" if succeeded else f"daemon unavailable: {version}",
        required=False,
    )


def checks() -> list[Check]:
    return [*(tool_check(tool) for tool in TOOLS), *rustup_check(), docker_check()]


def print_report(report: list[Check]) -> int:
    for check in report:
        if check.ok:
            state = "ok"
        elif check.required:
            state = "missing"
        else:
            state = "warning"
        print(f"[{state:7}] {check.label}: {check.detail}")

    required_failures = [check for check in report if check.required and not check.ok]
    warnings = [check for check in report if not check.required and not check.ok]
    print(
        f"doctor: {len(report) - len(required_failures) - len(warnings)} ok, "
        f"{len(required_failures)} required issue(s), {len(warnings)} warning(s)"
    )
    return 1 if required_failures else 0


def run(args: list[str]) -> bool:
    print("+ " + " ".join(args), flush=True)
    try:
        return subprocess.run(args, cwd=REPOSITORY, check=False).returncode == 0
    except OSError as error:
        print(f"failed to run {args[0]}: {error}", file=sys.stderr)
        return False


def install_rustup_requirements() -> bool:
    rustup = shutil.which("rustup")
    if rustup is None:
        print("rustup is required and must be installed manually", file=sys.stderr)
        return False

    required_msrv = msrv()
    commands = [
        [rustup, "component", "add", "--toolchain", "stable", "rustfmt", "clippy"],
        [rustup, "toolchain", "install", "nightly", "--profile", "minimal"],
        [rustup, "toolchain", "install", required_msrv, "--profile", "minimal"],
        [
            rustup,
            "target",
            "add",
            "--toolchain",
            required_msrv,
            *MSRV_TARGETS,
        ],
    ]
    succeeded = True
    for command in commands:
        succeeded = run(command) and succeeded
    return succeeded


def install_tools() -> bool:
    cargo = shutil.which("cargo")
    if cargo is None:
        print("cargo is required and must be installed manually", file=sys.stderr)
        return False

    succeeded = True
    for tool in TOOLS:
        status = tool_check(tool)
        if status.ok:
            print(f"already satisfied: {tool.label} ({status.detail})")
            continue
        command = [cargo, "install", tool.package, "--locked"]
        if tool.install_version is not None:
            command.extend(["--version", tool.install_version])
        if shutil.which(tool.executable) is not None:
            command.append("--force")
        succeeded = run(command) and succeeded
    return succeeded


def setup() -> int:
    print("Installing the toolchains and CLIs used by the repository gates...")
    installed = install_rustup_requirements()
    installed = install_tools() and installed
    print("\nFinal read-only diagnosis:")
    diagnosed = print_report(checks())
    return 0 if installed and diagnosed == 0 else 1


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[1] not in {"doctor", "setup"}:
        print("usage: dev_tools.py {doctor|setup}", file=sys.stderr)
        return 2
    if argv[1] == "setup":
        return setup()
    return print_report(checks())


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
