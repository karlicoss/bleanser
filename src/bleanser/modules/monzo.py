from bleanser.core.modules.json import Json, JsonNormaliser, delkeys


class Normaliser(JsonNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        delkeys(
            j,
            keys=[
                'account_balance',  # obvs flaky
                'suggested_tags',
                'website',
                #
                'address',
                'formatted',
                'logo',
                #
                ## flaky and useless
                'mastercard_lifecycle_id',
                'mastercard_clearing_message_id',
                'token_transaction_identifier',
                'tab_id',
                ##
                #
                'settled',
                'updated',
                'amount_is_pending',
                #
                'payee_id',  # odd but sometimes flaky
                'can_add_to_tab',
            ],
        )

        if isinstance(j, list):
            # old format, only transactions for one account
            return j

        # flatten out transactions
        for account, d in list(j.items()):
            transactions = d['data']['transactions']
            j[f'{account}_transactions'] = transactions
            del d['data']['transactions']
        return j


if __name__ == '__main__':
    Normaliser.main()
