"""Safe server-side git repository discovery and DAG browsing."""

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional


class RepositoryBrowser:
    """Manages registered DAG repositories under a configured repos root."""

    def __init__(self, repos_root: Path, registry_file: Path):
        self.repos_root = Path(repos_root).resolve()
        self.registry_file = Path(registry_file)
        self._dag_index_cache: Dict[str, List[Dict]] = {}

    def discover_repositories(self) -> List[str]:
        if not self.repos_root.exists() or not self.repos_root.is_dir():
            return []
        names = []
        for path in self.repos_root.iterdir():
            if path.is_dir() and (path / ".git").is_dir():
                names.append(path.name)
        return sorted(names)

    def registered_repositories(self) -> List[str]:
        return [
            name for name in self._load_registry()
            if self._is_valid_repo_name(name)
        ]

    def add_repository(self, name: str) -> None:
        self._repo_path(name)
        repositories = set(self._load_registry())
        repositories.add(name)
        self._save_registry(sorted(repositories))

    def remove_repository(self, name: str) -> None:
        repositories = [repo for repo in self._load_registry() if repo != name]
        self._save_registry(repositories)

    def build_dag_tree(self, name: str) -> List[Dict]:
        repo_path = self._registered_repo_path(name)
        root: Dict[str, Dict] = {}
        files: List[Path] = []
        for path in sorted(repo_path.rglob("*.py")):
            resolved = path.resolve()
            if self._is_within(resolved, repo_path):
                files.append(resolved.relative_to(repo_path))

        for relative_path in files:
            current = root
            parts = relative_path.parts
            for index, part in enumerate(parts):
                is_file = index == len(parts) - 1
                if is_file:
                    current[part] = {
                        "id": f"file:{relative_path.as_posix()}",
                        "label": part,
                        "path": relative_path.as_posix(),
                    }
                    continue
                current = current.setdefault(part, {})

        return self._nodes_from_mapping(root, prefix="")

    def build_dag_index(self, name: str, refresh: bool = False) -> List[Dict]:
        repo_path = self._registered_repo_path(name)
        if refresh:
            self.invalidate_dag_index(name)
        cache_key = f"{name}:{self._repo_revision(repo_path)}"
        if cache_key in self._dag_index_cache:
            return [dict(node) for node in self._dag_index_cache[cache_key]]

        tree = {"dirs": {}, "files": []}
        for path in sorted(repo_path.rglob("*.py"), key=lambda item: item.as_posix().lower()):
            resolved = path.resolve()
            if not self._is_within(resolved, repo_path) or not resolved.is_file():
                continue
            relative_path = resolved.relative_to(repo_path)
            current = tree
            for part in relative_path.parts[:-1]:
                current = current["dirs"].setdefault(part, {"dirs": {}, "files": []})
            current["files"].append(relative_path)

        metadata = self._git_metadata_for_python_files(repo_path)
        nodes = self._dag_index_nodes(repo_path, tree, (), None)
        for node in nodes:
            if node["type"] != "file":
                continue
            git_metadata = metadata.get(node["path"], {})
            message = git_metadata.get("git_message", "")
            commit_date = git_metadata.get("git_date", "")
            if commit_date:
                node["mtime"] = commit_date
                node["mtime_display"] = self._display_commit_date(commit_date)
            node["git_author"] = git_metadata.get("git_author") or "-"
            node["git_message"] = message or "-"
            node["git_message_short"] = message[:20] if message else "-"

        self._dag_index_cache = {
            key: value for key, value in self._dag_index_cache.items()
            if not key.startswith(f"{name}:")
        }
        self._dag_index_cache[cache_key] = [dict(node) for node in nodes]
        return nodes

    def invalidate_dag_index(self, name: Optional[str] = None) -> None:
        if name is None:
            self._dag_index_cache.clear()
            return
        self._dag_index_cache = {
            key: value for key, value in self._dag_index_cache.items()
            if not key.startswith(f"{name}:")
        }

    def resolve_dag_path(self, name: str, node_id: str) -> Path:
        repo_path = self._registered_repo_path(name)
        relative_path = node_id.removeprefix("file:")
        candidate = (repo_path / relative_path).resolve()
        if candidate.suffix != ".py":
            raise ValueError("Only .py DAG files are allowed")
        if not candidate.exists() or not candidate.is_file():
            raise ValueError(f"DAG file does not exist: {relative_path}")
        if not self._is_within(candidate, repo_path):
            raise ValueError(f"DAG file is outside repository: {relative_path}")
        return candidate

    def list_directory(self, name: str, relative_dir: str = "") -> Dict:
        repo_path = self._registered_repo_path(name)
        relative_dir = relative_dir.strip("/")
        current_path = (repo_path / relative_dir).resolve()
        if not self._is_within(current_path, repo_path):
            raise ValueError(f"Directory is outside repository: {relative_dir}")
        if not current_path.exists() or not current_path.is_dir():
            raise ValueError(f"Directory does not exist: {relative_dir}")

        current = current_path.relative_to(repo_path).as_posix()
        if current == ".":
            current = ""
        parent = None
        if current:
            parent_path = Path(current).parent
            parent = "" if parent_path.as_posix() == "." else parent_path.as_posix()

        directories = []
        files = []
        for child in sorted(current_path.iterdir(), key=lambda path: path.name.lower()):
            if child.name.startswith("."):
                continue
            resolved_child = child.resolve()
            if not self._is_within(resolved_child, repo_path):
                continue
            relative_child = resolved_child.relative_to(repo_path).as_posix()
            if child.is_dir():
                directories.append({"name": child.name, "path": relative_child})
            elif child.is_file() and child.suffix == ".py":
                files.append({
                    "name": child.name,
                    "node_id": f"file:{relative_child}",
                    "path": relative_child,
                })

        return {
            "current": current,
            "parent": parent,
            "directories": directories,
            "files": files,
        }

    def pull_repository(self, name: str) -> str:
        repo_path = self._registered_repo_path(name)
        result = subprocess.run(
            ["git", "-C", str(repo_path), "pull", "--ff-only"],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        )
        self.invalidate_dag_index(name)
        return "\n".join(part.strip() for part in [result.stdout, result.stderr] if part.strip())

    def pull_all(self) -> Dict[str, str]:
        return {name: self.pull_repository(name) for name in self.registered_repositories()}

    def repo_head_revision(self, name: str) -> str:
        return self._repo_revision(self._registered_repo_path(name))

    def _repo_path(self, name: str) -> Path:
        candidate = (self.repos_root / name).resolve()
        if candidate.parent != self.repos_root:
            raise ValueError(f"Repository must be a direct child of {self.repos_root}: {name}")
        if not candidate.is_dir() or not (candidate / ".git").is_dir():
            raise ValueError(f"Repository is not a git checkout: {name}")
        return candidate

    def _registered_repo_path(self, name: str) -> Path:
        if name not in self.registered_repositories():
            raise ValueError(f"Repository is not registered: {name}")
        return self._repo_path(name)

    def _is_valid_repo_name(self, name: str) -> bool:
        try:
            self._repo_path(name)
        except ValueError:
            return False
        return True

    def _load_registry(self) -> List[str]:
        if not self.registry_file.exists():
            return []
        try:
            data = json.loads(self.registry_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        repositories = data.get("repositories", [])
        if not isinstance(repositories, list):
            return []
        return [name for name in repositories if isinstance(name, str)]

    def _save_registry(self, repositories: List[str]) -> None:
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        self.registry_file.write_text(
            json.dumps({"repositories": repositories}, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _dag_index_nodes(self, repo_path: Path, tree: Dict, parts: tuple, parent: Optional[str]) -> List[Dict]:
        nodes = []
        for directory_name in sorted(tree["dirs"], key=str.lower):
            directory_parts = (*parts, directory_name)
            directory_path = "/".join(directory_parts)
            directory_tree = tree["dirs"][directory_name]
            node = {
                "id": f"dir:{directory_path}",
                "type": "dir",
                "name": directory_name,
                "path": directory_path,
                "parent": parent,
                "level": len(directory_parts) - 1,
                "children_count": len(directory_tree["dirs"]) + len(directory_tree["files"]),
                "mtime": "",
                "mtime_display": "-",
                "git_author": "-",
                "git_message": "-",
                "git_message_short": "-",
            }
            nodes.append(node)
            nodes.extend(self._dag_index_nodes(repo_path, directory_tree, directory_parts, node["id"]))

        for relative_path in sorted(tree["files"], key=lambda path: path.as_posix().lower()):
            path_text = relative_path.as_posix()
            parent_path = relative_path.parent.as_posix()
            parent_id = None if parent_path == "." else f"dir:{parent_path}"
            nodes.append({
                "id": f"file:{path_text}",
                "node_id": f"file:{path_text}",
                "type": "file",
                "name": relative_path.name,
                "path": path_text,
                "parent": parent_id,
                "level": len(relative_path.parts) - 1,
                "children_count": 0,
                "mtime": "",
                "mtime_display": "-",
                "git_author": "-",
                "git_message": "-",
                "git_message_short": "-",
            })
        return nodes

    def _git_metadata_for_python_files(self, repo_path: Path) -> Dict[str, Dict[str, str]]:
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_path), "log", "--format=format:%x1e%an%x1f%cI%x1f%s", "--name-only", "--"],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except (subprocess.SubprocessError, OSError):
            return {}
        if result.returncode != 0:
            return {}

        metadata: Dict[str, Dict[str, str]] = {}
        author = ""
        commit_date = ""
        message = ""
        for raw_line in result.stdout.split("\n"):
            line = raw_line.rstrip()
            if not line:
                continue
            if line.startswith("\x1e"):
                payload = line[1:]
                if "\x1f" in payload:
                    parts = payload.split("\x1f", 2)
                    author = parts[0]
                    commit_date = parts[1] if len(parts) > 1 else ""
                    message = parts[2] if len(parts) > 2 else ""
                else:
                    author, commit_date, message = payload, "", ""
                continue
            path = line.replace("\\", "/")
            if path.endswith(".py") and path not in metadata:
                metadata[path] = {
                    "git_author": author,
                    "git_date": commit_date,
                    "git_message": message,
                }
        return metadata

    def _display_commit_date(self, commit_date: str) -> str:
        return commit_date[:16].replace("T", " ") if len(commit_date) >= 16 else commit_date or "-"

    def _repo_revision(self, repo_path: Path) -> str:
        try:
            result = subprocess.run(
                ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
        except (subprocess.SubprocessError, OSError):
            return "unknown"
        return result.stdout.strip() if result.returncode == 0 and result.stdout.strip() else "unknown"

    def _nodes_from_mapping(self, mapping: Dict, prefix: str) -> List[Dict]:
        nodes = []
        for label in sorted(mapping):
            value = mapping[label]
            if "id" in value:
                nodes.append(value)
                continue
            path = f"{prefix}/{label}" if prefix else label
            nodes.append({
                "id": f"dir:{path}",
                "label": label,
                "children": self._nodes_from_mapping(value, path),
            })
        return nodes

    def _is_within(self, path: Path, base: Path) -> bool:
        try:
            path.relative_to(base)
        except ValueError:
            return False
        return True
