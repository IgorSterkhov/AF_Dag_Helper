"""Safe server-side git repository discovery and DAG browsing."""

import json
import subprocess
from pathlib import Path
from typing import Dict, List


class RepositoryBrowser:
    """Manages registered DAG repositories under a configured repos root."""

    def __init__(self, repos_root: Path, registry_file: Path):
        self.repos_root = Path(repos_root).resolve()
        self.registry_file = Path(registry_file)

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
        return "\n".join(part.strip() for part in [result.stdout, result.stderr] if part.strip())

    def pull_all(self) -> Dict[str, str]:
        return {name: self.pull_repository(name) for name in self.registered_repositories()}

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
