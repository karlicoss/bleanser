#!/usr/bin/env python3
from bleanser.core.json import delkeys
from bleanser.modules.json_new import Normaliser as JsonNormaliser, Json


REDDIT_IGNORE_KEYS = {
    ## TODO hmm maybe do something about these
    ## might be useful to keep
    'subreddit_subscribers',
    'subscribers',
    'ups',
    'score',
    'num_comments',
    'upvote_ratio',
    ###

    ## TODO ??
    'pwls', # TODO what is it??
    'likes', # todo?
    'wls', # TODO ???
    ##

    '_comments',
    'accept_chats',
    'accept_pms',
    'advertiser_category',
    'all_awardings',
    'allow_chat_post_creation',
    'allow_discovery',
    'allow_galleries',
    'allow_images',
    'allow_live_comments',
    'allow_polls',
    'allow_videogifs',
    'allow_videos',
    'allowed_galleries',
    'archived',
    'associated_award',
    'audience_target',
    'author_flair_background_color',
    'author_flair_css_class',
    'author_flair_richtext',
    'author_flair_template_id',
    'author_flair_text',
    'author_flair_text_color',
    'author_flair_type',
    'author_patreon_flair',
    'author_premium',
    'awarders',
    'banner_background_color',
    'banner_background_image',
    'banner_img',
    'banner_size',
    'can_assign_link_flair',
    'can_assign_user_flair',
    'can_gild',
    'collapse_deleted_comments',
    'collapsed', 'collapsed_reason', # todo potentially interesting?
    'comment_score_hide_mins',
    'community_icon',
    'content_categories',
    'crosspost_parent_list',
    'dash_url',
    'discussion_type',
    'emojis_custom_size',
    'emojis_enabled',
    'event_start', 'event_end', 'event_is_live',
    'free_form_reports',
    'gid_1',
    'gid_2',
    'gid_3',
    'gilded',
    'gildings',
    'has_menu_widget',
    'header_img',
    'header_size',
    'header_title',
    'hide_score',
    'hls_url',
    'icon_img',
    'icon_name',
    'icon_size',
    'icon_url',
    'is_chat_post_feature_enabled',
    'is_crosspostable',
    'is_crosspostable_subreddit',
    'is_robot_indexable',
    'is_self',
    'is_video',
    'key_color',
    'link_flair_css_class',
    'link_flair_enabled',
    'link_flair_position',
    'link_flair_richtext',
    'link_flair_template_id',
    'link_flair_text',
    'link_flair_type',
    'linked_identities',
    'media_embed',
    'media_metadata',
    'mobile_banner_image',
    'new',
    'no_follow',
    'oembed',
    'og_description', 'og_title',
    'original_content_tag_enabled',
    'over18',
    'over_18',
    'owner_id',
    'parent_whitelist_status', # some ads thing
    'password_set',
    'post_hint',
    'post_hint',
    'pref_no_profanity', 'pref_geopopular', 'pref_top_karma_subreddits',
    'primary_color',
    'pwls',
    'report_reasons',
    'restrict_commenting',
    'restrict_posting',
    'rte_mode',
    'score_hidden',
    'secure_media',
    'secure_media_embed',
    'send_replies',
    'show_media',
    'show_media_preview',
    'spoilers_enabled',
    'steward_report',
    'stickied',
    'submission_type',
    'submit_link_label',
    'submit_text_label',
    'suggested_comment_sort',
    'suggested_sort',
    'suggested_sort',
    'thumbnail',
    'thumbnail_height',
    'thumbnail_width',
    'top_awarded_type',
    'total_awards_received',
    'treatment_tags',
    'treatment_tags',
    'user_flair_richtext',
    'user_flair_template_id',
    'user_flair_text_color',
    'user_flair_type',
    'user_reports',
    'videostream_links_count',
    'whitelist_status',
    'whitelist_status', # some ads thing
    'wiki_enabled',


    # TODO ??
    # 'likes',
    # 'url', # ugh. changed from www.reddit.... to link without reddit domain
    # 'is_favorited',
    # 'is_subscriber',
    # 'domain',
    # should_archive_posts -- not sure?
    #
    #
    # subreddit_type: public/restricted -- actually quite useful info!
}

class Normaliser(JsonNormaliser):
    # filter out additions; keep the rest
    DIFF_FILTER =  '> '

    DELETE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        delkeys(j, keys=REDDIT_IGNORE_KEYS)
        return j


if __name__ == '__main__':
    Normaliser.main()


def test_reddit() -> None:
    from bleanser.tests.common import TESTDATA, actions, hack_attribute
    # TODO add a test for multiway

    data = TESTDATA / 'reddit'
    paths = list(sorted(data.glob('*.json*')))

    res = actions(paths=paths, Normaliser=Normaliser)

    assert [p.name for p in res.remaining] == [
        'reddit_20211227T164130Z.json',  # first in group
        'reddit_20211227T170106Z.json',  # saved item rolled over
        'reddit_20211227T171058Z.json',  # some saved items rolled over

        'reddit_20211227T173058Z.json',  # keeping boundary
        'reddit_20211230T034059Z.json',  # some items rolled over
        'reddit_20211230T035056Z.json',  # some things legit disappeared due to api limits

        'reddit_20211230T041057Z.json',  # keeping boundary for the next one
        'reddit_20220101T185059Z.json',  # subreddit description

        'reddit_20220101T191057Z.json',  # ??
        'reddit_20220101T192056Z.json',  # subreddit description changed
        'reddit_20220101T193109Z.json',  # also subreddit description

        'reddit_20220102T132059Z.json',  # ??
        'reddit_20220102T142057Z.json',  # author changed (likely deleted?)
        'reddit_20220102T164059Z.json',  # last in group
    ]
