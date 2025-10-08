from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    # even though we drop some of these, currently columns are dropped via erasing the content, not altering table
    # so need to keep here too
    ALLOWED_BLOBS = frozenset({
        ('channel_messages', 'attach'),

        ('messages', 'avatar'),
        ('messages', 'attach'),
        ('messages', 'carousel'),
        ('messages', 'nested'),
        ('messages', 'keyboard_buttons'),

        ('users', 'avatar'),
        ('users', 'image_status'),
        ('contacts', 'avatar'),
        ('groups', 'avatar'),

        ('dialogs', 'bar_buttons'),
        ('dialogs', 'chat_settings_members_active'),
        ('dialogs', 'chat_settings_admins'),
        ('dialogs', 'chat_settings_avatar'),
        ('dialogs', 'draft_msg'),
        ('dialogs', 'expire_msg_vk_ids'),
        ('dialogs', 'group_call_participants'),
        ('dialogs', 'keyboard_buttons'),
        ('dialogs', 'pinned_msg_attaches'),
        ('dialogs', 'pinned_msg_nested'),
        ('dialogs', 'pinned_carousel'),
        ('dialogs', 'unread_mention_msg_vk_ids'),

        ('mutual_friends', 'mutual_friends_ids'),
    })  # fmt: skip

    def is_vkim(self, c) -> bool:
        tables = Tool(c).get_tables()
        if 'messages' in tables:
            return True
        else:
            # otherwise must be vk.db
            return False

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        if self.is_vkim(c):
            msgs = tables['messages']
            assert 'vk_id' in msgs, msgs
            assert 'time' in msgs, msgs

            dialogs = tables['dialogs']
            assert 'id' in dialogs, dialogs
        else:
            users = tables['users']
            assert 'uid' in users, users
            assert 'firstname' in users, users

    def cleanup_vk_db(self, c) -> None:
        t = Tool(c)
        t.drop(table='friends_hints_order')
        t.drop_cols(
            table='users',
            cols=[
                # TODO hmm lately (202309), is_friend seems to be flaky for no reason? even where there are no status changes
                'last_updated',
                'photo_small',
                'lists',  # very flaky for some reason, sometimes just flips to 0??
                'name_r',  # seems derived from first/last name, and is very flaky
            ],
        )

    def cleanup(self, c) -> None:
        self.check(c)  # todo could also call 'check' after just in case

        if not self.is_vkim(c):
            self.cleanup_vk_db(c)
            return

        t = Tool(c)

        for table in [
            'peers_search_content',
            'peers_search_segments',
            'peers_search_segdir',
            'peers_search_docsize',
            'peers_search_stat',
            'messages_search_segments',
            'messages_search_segdir',
            'messages_search_docsize',
            'messages_search_stat',
            'messages_search_content',
            #
            'key_value',  # nothing interesting here
            'integer_generator',  # lol
            #
            ## no data, just some internal tracking
            'dialogs_history_count',
            'dialogs_history_meta',
            'dialog_weight',
            ##
        ]:
            t.drop(table=table)

        t.drop_cols(
            table='users',
            cols=[
                'avatar',  # flaky and no point tracking really
                'image_status',
                ## flaky timestamps
                'sync_time_overall',
                'sync_time_online',
                'online_last_seen',
                ##
                'online_app_id',
                'online_type',
            ],
        )

        t.drop_cols(
            table='contacts',
            cols=[
                'avatar',
                'sync_time',  # flaky
                'last_seen_status',  # flaky
            ],
        )

        t.drop_cols(
            table='dialogs',
            cols=[
                'sort_id_server',
                'sort_id_local',
                'weight',
                'read_till_in_msg_vk_id',
                'read_till_out_msg_vk_id',
                'last_msg_vk_id',
                'read_till_in_msg_vk_id_local',
                'read_till_in_msg_cnv_id',
                'read_till_out_msg_cnv_id',
                'last_msg_cnv_id',
                'count_unread',
                'count_unread_local',
                'keyboard_visible',
                'draft_msg',
                'bar_name',
                'bar_exists',
                'bar_buttons',
                'bar_text',
                'bar_icon',
            ],
        )

        t.drop_cols(
            table='messages',
            cols=[
                ## seems flaky -- not sure why, hard to tell since it's a binary blob
                'attach',
                'nested',
                ##
                'phase_id',  # not sure what is it, some internal stuff
            ],
        )

        t.drop_cols(
            table='groups',
            cols=[
                'avatar',
                'sync_time',
                'members_count',
            ],
        )


if __name__ == '__main__':
    Normaliser.main()
