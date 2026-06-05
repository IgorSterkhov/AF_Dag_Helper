"""Safe server-side DAG file discovery."""

from pathlib import Path
from typing import Iterable, List, Tuple


class ServerFileBrowser:
    """Lists and resolves DAG files from a small allowlist under project root."""

    def __init__(
        self,
        project_root: Path,
        allowed_dirs: Iterable[str] = ("Dags samples", "Dags for test"),
    ):
        self.project_root = Path(project_root).resolve()
        self.allowed_dirs: Tuple[str, ...] = tuple(allowed_dirs)

    def list_dag_files(self) -> List[str]:
        files: List[str] = []
        for dirname in self.allowed_dirs:
            base = (self.project_root / dirname).resolve()
            if not base.exists() or not base.is_dir():
                continue
            for path in sorted(base.rglob("*.py")):
                resolved = path.resolve()
                if self._is_allowed(resolved):
                    files.append(resolved.relative_to(self.project_root).as_posix())
        return files

    def resolve(self, relative_path: str) -> Path:
        candidate = (self.project_root / relative_path).resolve()
        if candidate.suffix != ".py":
            raise ValueError("Only .py DAG files are allowed")
        if not candidate.exists() or not candidate.is_file():
            raise ValueError(f"DAG file does not exist: {relative_path}")
        if not self._is_allowed(candidate):
            raise ValueError(f"DAG file is outside allowed directories: {relative_path}")
        return candidate

    def _is_allowed(self, path: Path) -> bool:
        for dirname in self.allowed_dirs:
            base = (self.project_root / dirname).resolve()
            try:
                path.relative_to(base)
            except ValueError:
                continue
            return True
        return False
