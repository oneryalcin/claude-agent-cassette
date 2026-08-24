"""Release artifacts must never contain local developer or agent state."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
from email.parser import Parser
from pathlib import Path, PurePosixPath

_IGNORED_COPY_DIRS = {
    ".git",
    ".mypy_cache",
    ".omx",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
}

_ALLOWED_SDIST_ROOTS = {
    ".gitignore",  # always included by Hatchling
    "LICENSE",  # project license file
    "PKG-INFO",  # generated distribution metadata
    "README.md",  # project readme
    "ROADMAP.md",
    "examples",
    "pyproject.toml",  # always included by Hatchling
    "src",
    "tests",
}


def _ignore_copy(_directory: str, names: list[str]) -> set[str]:
    return set(names) & _IGNORED_COPY_DIRS


def test_sdist_has_an_explicit_safe_file_set(tmp_path: Path) -> None:
    """Dirty-worktree files stay out even when they exist at build time."""
    source = Path(__file__).parent.parent
    project = tmp_path / "project"
    shutil.copytree(source, project, ignore=_ignore_copy)

    # These model the local files that caused the v0.5.0 release leak, plus
    # other common secrets and machine-local build inputs.
    (project / ".omx" / "logs").mkdir(parents=True)
    (project / ".omx" / "logs" / "turns.jsonl").write_text("PRIVATE AGENT LOG\n")
    (project / ".env").write_text("ANTHROPIC_API_KEY=DO_NOT_PACKAGE\n")
    (project / "uv.lock").write_text("LOCAL LOCKFILE\n")

    outdir = tmp_path / "dist"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--sdist",
            "--no-isolation",
            "--outdir",
            str(outdir),
        ],
        cwd=project,
        check=True,
        capture_output=True,
        text=True,
    )

    [archive] = outdir.glob("*.tar.gz")
    with tarfile.open(archive) as sdist:
        members = [PurePosixPath(name) for name in sdist.getnames()]
        [pkg_info_member] = [
            member for member in sdist.getmembers() if PurePosixPath(member.name).name == "PKG-INFO"
        ]
        pkg_info_file = sdist.extractfile(pkg_info_member)
        assert pkg_info_file is not None
        pkg_info = Parser().parsestr(pkg_info_file.read().decode())

    roots = {path.parts[1] for path in members if len(path.parts) > 1}
    assert roots <= _ALLOWED_SDIST_ROOTS
    assert ".omx" not in roots
    assert ".env" not in roots
    assert "uv.lock" not in roots

    requirements = pkg_info.get_all("Requires-Dist", [])
    [sdk_requirement] = [
        requirement for requirement in requirements if requirement.startswith("claude-agent-sdk")
    ]
    assert ">=0.2.82" in sdk_requirement
    assert "<0.2.143" in sdk_requirement

    [mcp_requirement] = [requirement for requirement in requirements if requirement.startswith("mcp")]
    assert ">=1" in mcp_requirement
    assert "<2" in mcp_requirement
