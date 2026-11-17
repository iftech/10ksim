import subprocess

from src.deployments.core.code_version import code_version


def _git(repo, *args):
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t"]
        + ["-c", "commit.gpgsign=false", *args],
        check=True,
        capture_output=True,
    )


def test_records_whether_the_code_was_clean_and_pushed(tmp_path):
    remote, repo = tmp_path / "remote.git", tmp_path / "repo"
    _git(tmp_path, "init", "--bare", "-b", "main", str(remote))
    _git(tmp_path, "init", "-b", "main", str(repo))
    _git(repo, "remote", "add", "origin", str(remote))
    (repo / "a.py").write_text("x = 1\n")
    _git(repo, "add", "a.py")
    _git(repo, "commit", "-m", "a")

    assert code_version(repo)["pushed"] is False

    _git(repo, "push", "-u", "origin", "main")
    (repo / "a.py").write_text("x = 2\n")
    assert code_version(repo)["clean"] is False

    _git(repo, "checkout", "a.py")
    (repo / "notes.txt").write_text("untracked\n")
    code = code_version(repo)
    assert (code["clean"], code["pushed"], code["branch"]) == (True, True, "main")
