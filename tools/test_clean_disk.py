import contextlib
from io import StringIO
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

import clean_disk


class CleanDiskTests(unittest.TestCase):
    def test_main_continues_after_one_tree_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            blocked = root / "blocked"
            removable = root / "removable"
            blocked.mkdir()
            removable.mkdir()

            real_rmtree = shutil.rmtree

            def rmtree(path, *args, **kwargs):
                if Path(path) == blocked:
                    raise PermissionError("tree is locked")
                return real_rmtree(path, *args, **kwargs)

            stdout = StringIO()
            stderr = StringIO()
            with (
                mock.patch.object(clean_disk, "target_roots", return_value=[root / "target"]),
                mock.patch.object(
                    clean_disk,
                    "incremental_directories",
                    return_value=[blocked, removable],
                ),
                mock.patch.object(clean_disk.shutil, "rmtree", side_effect=rmtree),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                result = clean_disk.main()

            self.assertEqual(result, 0)
            self.assertTrue(blocked.exists())
            self.assertFalse(removable.exists())
            self.assertIn(f"skip tree after error: {blocked}: tree is locked", stderr.getvalue())
            self.assertIn("clean-disk removed 1 disposable directory", stdout.getvalue())

    def test_readonly_callback_retries_operation_after_chmod(self) -> None:
        for callback in (clean_disk._rmtree_onexc, clean_disk._rmtree_onerror):
            with self.subTest(callback=callback.__name__):
                calls = []

                def remove(path):
                    calls.append(("remove", path))

                with mock.patch.object(clean_disk.os, "chmod") as chmod:
                    callback(remove, "locked.txt", PermissionError("read-only"))

                chmod.assert_called_once_with("locked.txt", clean_disk.stat.S_IWRITE)
                self.assertEqual(calls, [("remove", "locked.txt")])


if __name__ == "__main__":
    unittest.main()
