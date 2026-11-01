import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("restore_scheduled_artifact.sh")
FAKE_GH = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

root = Path(os.environ["FAKE_GH_ROOT"])
args = sys.argv[1:]
with (root / "calls.jsonl").open("a") as output:
    output.write(json.dumps(args) + "\n")
if args[0] == "api":
    if os.environ.get("FAIL_API") == "1":
        sys.exit(41)
    print((root / "pages.json").read_text())
elif args[:2] == ["run", "download"]:
    destination = Path(args[args.index("--dir") + 1])
    (destination / "baseline").write_text("retained data")
    if os.environ.get("FAIL_DOWNLOAD") == "1":
        sys.exit(42)
else:
    sys.exit(43)
'''


def artifact(run_id: int, branch: str = "main", **changes) -> dict:
    result = {
        "id": run_id,
        "name": "saved-state",
        "expired": False,
        "created_at": f"2026-10-{run_id:02d}T00:00:00Z",
        "workflow_run": {
            "id": run_id,
            "repository_id": 123,
            "head_repository_id": 123,
            "head_branch": branch,
        },
    }
    result.update(changes)
    return result


class RestoreScheduledArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        gh = self.root / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(0o755)
        self.destination = self.root / "restored data"
        self.destination.mkdir()
        (self.destination / "seed").write_text("seed data")
        (self.destination / "baseline").write_text("cached data")
        self.env = {
            **os.environ,
            "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}",
            "FAKE_GH_ROOT": str(self.root),
            "GITHUB_REPOSITORY": "owner/project",
            "GITHUB_REPOSITORY_ID": "123",
            "GITHUB_REF_NAME": "main",
            "DEFAULT_BRANCH": "main",
            "GITHUB_RUN_ID": "99",
        }

    def restore(self, *pages: list[dict]) -> subprocess.CompletedProcess:
        (self.root / "pages.json").write_text(
            json.dumps([{"artifacts": page} for page in pages])
        )
        return subprocess.run(
            ["bash", str(SCRIPT), "saved-state", str(self.destination)],
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
        )

    def calls(self) -> list[list[str]]:
        return [
            json.loads(line)
            for line in (self.root / "calls.jsonl").read_text().splitlines()
        ]

    def test_first_run_keeps_cache_and_seeds(self) -> None:
        result = self.restore([])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No retained artifact", result.stdout)
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual((self.destination / "baseline").read_text(), "cached data")
        self.assertEqual((self.destination / "seed").read_text(), "seed data")

    def test_latest_artifact_across_pages_merges_with_existing_data(self) -> None:
        result = self.restore([artifact(1)], [artifact(3), artifact(2)])
        self.assertEqual(result.returncode, 0, result.stderr)
        query, download = self.calls()
        self.assertIn("--paginate", query)
        self.assertIn("--slurp", query)
        self.assertIn("name=saved-state", query)
        self.assertIn("repos/owner/project/actions/artifacts", query)
        self.assertEqual(download[:3], ["run", "download", "3"])
        self.assertEqual(download[3:7], ["--repo", "owner/project", "--name", "saved-state"])
        self.assertEqual((self.destination / "baseline").read_text(), "retained data")
        self.assertEqual((self.destination / "seed").read_text(), "seed data")
        self.assertFalse(Path(download[-1]).exists())

    def test_current_branch_precedes_newer_default_branch(self) -> None:
        self.env["GITHUB_REF_NAME"] = "feature/corpus"
        result = self.restore([artifact(7), artifact(2, "feature/corpus")])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls()[1][2], "2")

    def test_default_branch_supplies_fallback(self) -> None:
        self.env["GITHUB_REF_NAME"] = "feature/corpus"
        result = self.restore([artifact(7), artifact(9, "another-branch")])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls()[1][2], "7")

    def test_unrelated_expired_fork_and_current_run_artifacts_are_excluded(self) -> None:
        fork = artifact(4)
        fork["workflow_run"]["head_repository_id"] = 456
        other_repo = artifact(5)
        other_repo["workflow_run"]["repository_id"] = 456
        result = self.restore([
            artifact(1, expired=True),
            artifact(2, "another-branch"),
            artifact(3, name="another-artifact"),
            fork,
            other_repo,
            artifact(99),
        ])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.calls()), 1)
        self.assertEqual((self.destination / "baseline").read_text(), "cached data")

    def test_api_failure_is_not_a_cold_start(self) -> None:
        self.env["FAIL_API"] = "1"
        result = self.restore([])
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("No retained artifact", result.stdout)
        self.assertEqual(len(self.calls()), 1)

    def test_download_failure_preserves_existing_data(self) -> None:
        self.env["FAIL_DOWNLOAD"] = "1"
        result = self.restore([artifact(1)])
        self.assertEqual(result.returncode, 42)
        self.assertEqual((self.destination / "baseline").read_text(), "cached data")
        self.assertFalse(Path(self.calls()[1][-1]).exists())

    def test_restore_creates_missing_destination(self) -> None:
        self.destination = self.root / "new" / "corpus"
        result = self.restore([artifact(1)])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.destination / "baseline").read_text(), "retained data")


if __name__ == "__main__":
    unittest.main()
