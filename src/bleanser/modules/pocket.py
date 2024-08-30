from bleanser.core.modules.json import Json, JsonNormaliser


class Normaliser(JsonNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        del j['since']  # flaky
        return j


if __name__ == '__main__':
    Normaliser.main()
