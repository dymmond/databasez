#!/usr/bin/env python
from __future__ import annotations

import argparse
import shutil
import threading
from pathlib import Path

from docs_pipeline import DocsPipelineError, prepare_docs_tree, run_zensical

ROOT_DIR = Path(__file__).resolve().parent.parent
SOURCE_DOCS_DIR = ROOT_DIR / "docs"
GENERATED_DOCS_DIR = SOURCE_DOCS_DIR / "generated"
DEFAULT_CONFIG_FILE = ROOT_DIR / "mkdocs.yml"
DEFAULT_SITE_DIR = ROOT_DIR / "site"
DEFAULT_CACHE_DIR = ROOT_DIR / ".cache"
SOURCE_DOCS_SNIPPETS_DIR = ROOT_DIR / "docs_src"


def _snapshot(paths: list[Path]) -> dict[str, int]:
    state: dict[str, int] = {}
    generated_tmp_dir = SOURCE_DOCS_DIR / f".{GENERATED_DOCS_DIR.name}.tmp"
    excluded_roots = [GENERATED_DOCS_DIR.resolve(), generated_tmp_dir.resolve()]
    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            resolved = path.resolve()
            if any(resolved.is_relative_to(root) for root in excluded_roots):
                continue
            state[str(resolved)] = path.stat().st_mtime_ns
            continue
        for candidate in sorted(path.rglob("*")):
            if candidate.is_file():
                resolved = candidate.resolve()
                if any(resolved.is_relative_to(root) for root in excluded_roots):
                    continue
                state[str(resolved)] = candidate.stat().st_mtime_ns
    return state


def _resolve_config(config_file: str) -> Path:
    config_path = (ROOT_DIR / config_file).resolve()
    if not config_path.is_file():
        raise DocsPipelineError(f"Config file not found: {config_path}")
    return config_path


def _prepare() -> list[Path]:
    generated = prepare_docs_tree(SOURCE_DOCS_DIR, GENERATED_DOCS_DIR)
    print(f"Prepared {len(generated)} docs file(s) in {GENERATED_DOCS_DIR}")
    return generated


def _watch_sources(stop_event: threading.Event, interval: float = 0.5) -> None:
    watch_paths = [SOURCE_DOCS_DIR, SOURCE_DOCS_SNIPPETS_DIR]
    previous = _snapshot(watch_paths)
    while not stop_event.wait(interval):
        current = _snapshot(watch_paths)
        if current == previous:
            continue
        previous = current
        try:
            _prepare()
            print("Docs refreshed")
        except DocsPipelineError as exc:
            print(f"Docs refresh failed: {exc}")


def cmd_prepare(_: argparse.Namespace) -> None:
    _prepare()


def cmd_clean(_: argparse.Namespace) -> None:
    for path in [GENERATED_DOCS_DIR, DEFAULT_SITE_DIR, DEFAULT_CACHE_DIR]:
        shutil.rmtree(path, ignore_errors=True)
    print("Removed docs artifacts")


def cmd_build(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args.config_file)
    _prepare()
    run_zensical(
        project_root=ROOT_DIR,
        config_file=config_path,
        command="build",
        clean=args.clean,
    )
    print("Docs built with Zensical")


def cmd_serve(args: argparse.Namespace) -> None:
    config_path = _resolve_config(args.config_file)
    _prepare()

    stop_event = threading.Event()
    watch_thread: threading.Thread | None = None
    if args.watch_sources:
        watch_thread = threading.Thread(
            target=_watch_sources,
            args=(stop_event,),
            daemon=True,
        )
        watch_thread.start()
        print(f"Watching source docs for changes: {SOURCE_DOCS_DIR}, {SOURCE_DOCS_SNIPPETS_DIR}")

    try:
        run_zensical(
            project_root=ROOT_DIR,
            config_file=config_path,
            command="serve",
            dev_addr=args.dev_addr,
            open_browser=args.open,
        )
    finally:
        stop_event.set()
        if watch_thread is not None:
            watch_thread.join(timeout=1.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Documentation workflow for databasez.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Generate build-ready Markdown.")
    prepare.set_defaults(handler=cmd_prepare)

    clean = subparsers.add_parser("clean", help="Remove generated docs artifacts.")
    clean.set_defaults(handler=cmd_clean)

    build = subparsers.add_parser("build", help="Prepare docs and run `zensical build`.")
    build.add_argument(
        "-f",
        "--config-file",
        default=str(DEFAULT_CONFIG_FILE.relative_to(ROOT_DIR)),
        help="Path to docs config file relative to repository root.",
    )
    build.add_argument(
        "--clean",
        action="store_true",
        help="Clean Zensical cache before build.",
    )
    build.set_defaults(handler=cmd_build)

    serve = subparsers.add_parser("serve", help="Prepare docs and run `zensical serve`.")
    serve.add_argument(
        "-f",
        "--config-file",
        default=str(DEFAULT_CONFIG_FILE.relative_to(ROOT_DIR)),
        help="Path to docs config file relative to repository root.",
    )
    serve.add_argument(
        "-a",
        "--dev-addr",
        default="127.0.0.1:8000",
        help="Address and port for local docs server.",
    )
    serve.add_argument(
        "--open",
        action="store_true",
        help="Open docs in the default browser.",
    )
    serve.add_argument(
        "--watch-sources",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Watch docs and docs_src, regenerating docs/generated automatically.",
    )
    serve.set_defaults(handler=cmd_serve)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    try:
        main()
    except DocsPipelineError as exc:
        raise SystemExit(str(exc)) from exc
