from bleanser.core.modules.xml import Normaliser as XmlNormaliser


class Normaliser(XmlNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, t):
        for key in [
            'average_rating',
            'text_reviews_count',
            'ratings_count',
            'book/description',  # volatile
        ]:
            for x in t.findall('.//' + key):
                x.getparent().remove(x)
        return t


if __name__ == '__main__':
    Normaliser.main()
