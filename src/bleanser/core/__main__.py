# TODO hmm so we kind of need a specific Normaliser for bleanser, so calling
# python3 -m bleanser.core (or just -m bleanser) doesn't make much sense
# it could probs take in module name, and then call it? like python3 -m bleanser modules.xxx
# but it's the same as calling python -m bleanser.modules.xxx
# TODO maybe this thing could do module discovery or something?
def main() -> None:
    pass


if __name__ == '__main__':
    # FIXME warn if we're running this command? kinda confusing otherwise
    main()
