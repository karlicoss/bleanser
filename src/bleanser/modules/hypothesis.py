from bleanser.core.modules.json import JsonNormaliser, Json


class Normaliser(JsonNormaliser):
    def cleanup(self, j: Json) -> Json:
        if isinstance(j, list):
            # old export format
            return j
        del j['profile']['features']  # flaky
        return j

if __name__ == '__main__':
    Normaliser.main()
