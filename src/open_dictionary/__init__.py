from .cli import main as _cli_main

def main() -> None:
    raise SystemExit(_cli_main())
