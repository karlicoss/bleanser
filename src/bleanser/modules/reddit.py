from itertools import chain

from bleanser.core.modules.json import Json, JsonNormaliser, delkeys

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
    'pref_no_profanity', 'pref_geopopular', 'pref_top_karma_subreddits',
    'primary_color',
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
    'thumbnail',
    'thumbnail_height',
    'thumbnail_width',
    'top_awarded_type',
    'total_awards_received',
    'treatment_tags',
    'user_flair_richtext',
    'user_flair_template_id',
    'user_flair_text_color',
    'user_flair_type',
    'user_reports',
    'videostream_links_count',
    'whitelist_status',  # some ads thing
    'wiki_enabled',
    'snoovatar_img',
    'snoovatar_size',
    'allow_talks',

    ## very flaky
    'link_flair_background_color',
    'link_flair_text_color',
    'call_to_action',  # sometimes null, sometimes not present?
    ##
    ##

    ## nothing interesting, some subreddit settings
    'allowed_media_in_comments',
    'comment_contribution_settings',
    'should_archive_posts',
    ##

    'awardee_karma',  # sometimes goes to 0 for no reason

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
    # profile -> link_karma, comment_karma -- probs useful to keep
    #
    # TODO maybe, num_crossposts? have only seen once so far
}  # fmt: skip


class Normaliser(JsonNormaliser):
    # NOTE: we don't want to prune dominated/use multiway in reddit, because that way we lose timestamps for changes!!!
    PRUNE_DOMINATED = False

    def cleanup(self, j: Json) -> Json:
        delkeys(j, keys=REDDIT_IGNORE_KEYS)

        # hmm, 'created' changes all the time for some reason starting from 20181124201020
        # https://www.reddit.com/r/redditdev/comments/29991t/whats_the_difference_between_created_and_created/ciiuk24/
        # ok, it's broken, should use created_utc instead
        for v in j.values():
            if not isinstance(v, list):
                continue
            for i in v:
                if 'created_utc' in i:
                    i.pop('created', None)

                i.pop('subreddit_type', None)

        ## karma is flaky, goes up and down even without actual votes
        ## so make it a bit smoother
        profile = j['profile']
        for kf in ['link_karma', 'total_karma']:
            k = profile.get(kf)
            if k is not None:
                profile[kf] = k // 10 * 10
        # ugh, total karma is flaking between two values for me consistently
        # but removing it completely only gets rid of 10% of files?
        ##

        for u in chain(j['upvoted'], j['downvoted']):
            ## not sure what it is, but flaky from "" to null
            u.pop('category', None)

            ## very flaky, often goes from gfycat.com to null
            media = u.get('media')
            if media is not None:
                media.pop('type', None)
            if media is None or len(media) == 0:
                u.pop('media', None)

            # gallery_data is sometimes flaking to none

        for s in j['subreddits']:
            # volatile when we've got enough subreddits -- not worth keeping
            s.pop('description', None)
            s.pop('public_description', None)
            s.pop('public_description_html', None)
            s.pop('submit_text', None)
            s.pop('submit_text_html', None)
            s.pop('disable_contributor_requests', None)

        return j


if __name__ == '__main__':
    Normaliser.main()


def test_reddit_1() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions
    # TODO add a test for multiway

    data = TESTDATA / 'reddit'
    paths = sorted(data.glob('*.json*'))

    res = actions(paths=paths, Normaliser=Normaliser)

    assert [p.name for p in res.remaining] == [
        'reddit_20211227T164130Z.json',  # first in group
        'reddit_20211227T170106Z.json',  # saved item rolled over
        'reddit_20211227T171058Z.json',  # some saved items rolled over

        'reddit_20211227T173058Z.json',  # keeping boundary
        'reddit_20211230T034059Z.json',  # some items rolled over
        'reddit_20211230T035056Z.json',  # some things legit disappeared due to api limits

        'reddit_20220102T132059Z.json',  # boundary for the next one
        'reddit_20220102T142057Z.json',  # author changed (likely deleted?)
        'reddit_20220102T164059Z.json',  # last in group
    ]  # fmt: skip


def test_reddit_2() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions

    data = TESTDATA / 'reddit2'
    paths = sorted(data.glob('*.json*'))

    res = actions(paths=paths, Normaliser=Normaliser)
    # note: fieles appear to be spaced out by 20 mins instead of 10 (backup frequency)
    # this is ok, because I temporarily moved every other file away in the absence of bleanser
    assert [p.name for p in res.remaining] == [
        'reddit_20210803T121056Z.json',

        # ^v -- identical

        'reddit_20210803T213053Z.json',

        # here: some saved items rolled over
        'reddit_20210803T215050Z.json',

        'reddit_20210804T213055Z.json',
    ]  # fmt: skip
