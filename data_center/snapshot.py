import data_center.data_service as bsc
import ext_service.facebook as fb


class SnapShot:
    def __init__(self):
        # ad_id -> [campaign_id, ad_set_id]
        self.active_ads_dict = dict()
        self.active_ads_relevance_score = dict()
        self.active_ads_bid_amount = dict()
        # ad_id -> [campaign_id, ad_set_id]
        self.inactive_ads_dict = dict()
        self.inactive_ads_relevance_score = dict()
        self.pending_campaigns = list()
        self.disapproved_campaigns = list()
        self.active_ads_indexes = dict()
        self.active_ads_bid_amount = dict()

    def update(self, today):
        self.active_ads_dict = bsc.get_active_ads_by_delivery()
        self.active_ads_relevance_score = fb.get_ads_relevance(list(self.active_ads_dict.keys()))
        ad_sets_id = list()
        for key in self.active_ads_dict:
            ad_sets_id.append(self.active_ads_dict[key][1])
        tmp = fb.get_adset_bid_amount(ad_sets_id)
        self.active_ads_bid_amount = dict()
        for key in self.active_ads_dict:
            self.active_ads_bid_amount[key] = tmp[self.active_ads_dict[key][1]]
        self.inactive_ads_dict = bsc.get_paused_ads_by_delivery()
        self.inactive_ads_relevance_score = fb.get_ads_relevance(list(self.inactive_ads_dict.keys()))
        self.pending_campaigns = bsc.get_pending_view_campaigns_by_delivery()
        self.disapproved_campaigns = bsc.get_disapproved_campaigns_by_delivery()
        self.active_ads_indexes = fb.get_ads_insights(list(self.active_ads_dict.keys()), since=today, until=today)


snap_data = SnapShot()
