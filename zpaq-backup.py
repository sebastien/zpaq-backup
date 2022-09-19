#!/usr/bin/env python
from pathlib import Path
from typing import Optional, Iterator, Union, Iterable, TypeVar
from subprocess import Popen, PIPE
from fnmatch import fnmatch
from glob import glob
from shutil import which
import os

T = TypeVar("")

# --
# A simple utility to use ZPaq to create incremental backups.


def matches(
    value: Union[str, Iterable[str]],
    accepts: Optional[list[str]] = None,
    rejects: Optional[list[str]] = None,
) -> bool:
    """Tells if the given value passes the `accepts` and `rejects` filters."""
    if isinstance(value, str):
        for pat in accepts or ():
            if fnmatch(value, pat):
                return True
        for pat in rejects or ():
            if fnmatch(value, pat):
                return False
        return True
    else:
        for v in value:
            if not matches(v, accepts, rejects):
                return False
        return True


def dotfile(name: str, base: Optional[Path] = None) -> Optional[Path]:
    """Looks for the file `name` in the current directory or its ancestors"""
    user_home: Optional[str] = os.getenv("HOME")
    path = Path(base or ".").absolute()
    while path != path.parent:
        if (loc := path / name).exists():
            return loc
        if path != user_home:
            path = path.parent
        else:
            break
    return None


def gitignored(path: Optional[Path] = None) -> list[str]:
    """Returns the list of patterns that are part of the `gitignore` file."""
    path = dotfile(".gitignore") if not path else path
    res: list[str] = []
    if path and path.exists():
        with open(path, "rt") as f:
            for pattern in f.readlines():
                pattern = pattern.strip().rstrip("\n")
                if pattern.startswith("#"):
                    continue
                res.append(pattern)
    return res


def normpath(path: str) -> Path:
    return Path(os.path.normpath(os.path.expanduser(os.path.expandvars(path))))


def walk(
    path: Path, accepts: Optional[list[str]] = None, rejects: Optional[list[str]] = None
) -> Iterator[Path]:
    """Yields a stream of `Path` relative to `path`, matching the accepts and rejects
    patterns."""
    # TODO: This may be a bit slow as we're going to recurse event unwanted directories
    implementation = 1
    if implementation == 0:
        for root, dirs, files in os.walk(path):
            root = Path(root)
            if not matches(os.path.basename(root), accepts=accepts, rejects=rejects):
                continue
            for file_path in files:
                if matches(file_path, accepts=accepts, rejects=rejects):
                    yield (root / file_path)
    else:
        queue: list[str] = [path]
        max_queue: int = 0
        while queue:
            # NOTE: This is a good measure if things get slow
            max_queue = max(max_queue, len(queue))
            base_path: str = queue.pop()
            for rel_path in os.listdir(base_path):
                if matches(rel_path, accepts, rejects):
                    abs_path = f"{base_path}/{rel_path}"
                    if os.path.isdir(abs_path):
                        queue.append(abs_path)
                    else:
                        yield Path(abs_path)


def walk_many(
    paths: list[Path],
    accepts: Optional[list[str]] = None,
    rejects: Optional[list[str]] = None,
) -> Iterator[Path]:
    for path in paths:
        yield from walk(path, accepts, rejects)


def batch(stream: Iterable[T], count: int = 10_000) -> Iterable[list[T]]:
    batch: list[T] = []
    n: int = 0
    while True:
        try:
            batch.append(next(stream))
            if (n := n + 1) == count:
                yield batch
                batch = []
                n = 0
        except StopIteration:
            yield batch
            break


def zpaq_path(archive: Path, incremental: bool = True) -> str:
    """Returns the path to the archive, adding the wildcards for chunk support."""
    return (
        f"{archive.absolute().parent}/{archive.name.split('.')[0]}-???.zpaq"
        if incremental
        else str(archive.absolute())
    )


def zpaq_increments(archive: Path, incremental: bool = True) -> str:
    return (
        increments
        if (increments := [_ for _ in glob(zpaq_path(archive, True))])
        else [archive]
        if archive.exists()
        else []
    )


# --
# ZPaq functionality
def zpaq_add(archive: Path, root: Path, contents: Iterable[Path]) -> int:
    zpaq = which("zpaq")
    archive_path = f"{archive.absolute().parent}/{archive.name.split('.')[0]}-???.zpaq"
    for files in batch(contents, 10_000):
        cmd = [zpaq, "add", archive_path]
        print(f">>> {cmd} with {len(files)} files in {root}: {archive_path}")
        proc = Popen(
            cmd + [str(_.relative_to(root)) for _ in files],
            stdout=PIPE,
            stderr=PIPE,
            cwd=str(root),
        )
        while line := proc.stdout.readline():
            print("OUT", line)
        if proc.returncode is None:
            proc.communicate(timeout=15)
        if (status := proc.returncode) != 0:
            print(f"FTL {status}")
            return status
        # print(proc)


def zpaq_list(archive: Path) -> Iterable[Path]:
    pass
    # - 2022-09-19 05:52:34          625  0644 /home/spierre/Workspace/tickerapi--main/.sharedsecrets/user/spierre.pubkey


if __name__ == "__main__":
    paths = [
        normpath(_)
        for _ in [
            "$HOME/.ssh",
            "$HOME/Workspace",
        ]
    ]
    root = os.path.commonpath(paths)
    rejects = gitignored()
    # zpaq_add(Path("xxx-archive.zpaq"), root, walk_many([normpath(_) for _ in paths], rejects=rejects))
    print(zpaq_increments(Path("xxx-archive.zpaq")))

# EOF
