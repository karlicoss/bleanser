import sys


def main() -> None:
    print(
        """
This command is a NO-OP.
You should run bleanser via a specific module, e.g:
    python3 -m bleanser.modules.pocket ...
""",
        file=sys.stderr,
    )
    # TODO maybe this thing could do module discovery or something?
    sys.exit(1)


if __name__ == '__main__':
    main()
