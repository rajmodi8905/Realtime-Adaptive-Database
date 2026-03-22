"""CLI entry point for terminal-driven Assignment 2 CRUD queries.

Examples:
  python -m src.cli query --op read --payload '{"fields":["username"],"filters":{"username":"username_1"},"limit":5}'
  python -m src.cli query --op update --payload-file payloads/update.json
  python -m src.cli query --op delete --interactive
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.a2.contracts import CrudOperation
from src.a2.orchestrator import Assignment2Pipeline
from src.config import get_config


def _parse_payload(args: argparse.Namespace) -> dict:
	if args.payload and args.payload_file:
		raise ValueError("Use either --payload or --payload-file, not both.")

	if args.payload:
		try:
			payload = json.loads(args.payload)
		except json.JSONDecodeError as exc:
			raise ValueError(f"Invalid JSON passed to --payload: {exc}") from exc
	elif args.payload_file:
		path = Path(args.payload_file)
		if not path.exists():
			raise ValueError(f"Payload file not found: {path}")
		try:
			payload = json.loads(path.read_text(encoding="utf-8"))
		except json.JSONDecodeError as exc:
			raise ValueError(f"Invalid JSON in payload file '{path}': {exc}") from exc
	elif args.interactive:
		print("Paste query payload JSON, then press Ctrl+D:")
		raw = sys.stdin.read()
		try:
			payload = json.loads(raw)
		except json.JSONDecodeError as exc:
			raise ValueError(f"Invalid interactive JSON payload: {exc}") from exc
	else:
		raise ValueError("Provide one of --payload, --payload-file, or --interactive.")

	if not isinstance(payload, dict):
		raise ValueError("Payload must be a JSON object.")
	return payload


def _run_query(args: argparse.Namespace) -> int:
	cfg = get_config()
	pipeline = Assignment2Pipeline(cfg)

	try:
		payload = _parse_payload(args)
		operation = CrudOperation(args.op.lower())
		result = pipeline.execute_operation(operation, payload)

		print(json.dumps(result, indent=2, default=str))
		return 0 if result.get("status") != "error" else 1
	except ValueError as exc:
		print(f"FAIL: {exc}")
		return 1
	except Exception as exc:  # noqa: BLE001
		print(f"FAIL: query execution failed: {exc}")
		return 1
	finally:
		pipeline.close()


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Terminal interface for A2 CRUD queries")
	subparsers = parser.add_subparsers(dest="command", required=True)

	query_parser = subparsers.add_parser(
		"query",
		help="Execute one metadata-driven CRUD operation",
	)
	query_parser.add_argument(
		"--op",
		required=True,
		choices=[op.value for op in CrudOperation],
		help="Operation type: create/read/update/delete",
	)
	query_parser.add_argument(
		"--payload",
		help="Inline JSON payload for the operation",
	)
	query_parser.add_argument(
		"--payload-file",
		help="Path to JSON payload file",
	)
	query_parser.add_argument(
		"--interactive",
		action="store_true",
		help="Read JSON payload from terminal stdin",
	)

	return parser


def main() -> int:
	parser = build_parser()
	args = parser.parse_args()

	if args.command == "query":
		return _run_query(args)

	print(f"FAIL: Unsupported command '{args.command}'")
	return 1


if __name__ == "__main__":
	raise SystemExit(main())
