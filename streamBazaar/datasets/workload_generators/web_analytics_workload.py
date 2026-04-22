from typing import Dict, Iterator

from .base import BaseWorkloadGenerator, WorkloadMetadata, default_state_sizes


class WebAnalyticsWorkloadGenerator(BaseWorkloadGenerator):
    def __init__(self, tenant_id: str, replay_window_compression: float, record_source: Iterator[Dict]) -> None:
        metadata = WorkloadMetadata(
            name="web_analytics",
            dataset="web-analytics",
            operator_count=12,
            priority="low",
            operators=[
                "raw_parser",
                "bot_filter",
                "sessionizer",
                "url_normalizer",
                "geo_enrichment",
                "device_enrichment",
                "campaign_join",
                "ctr_feature_builder",
                "rolling_aggregation",
                "window_ranker",
                "report_formatter",
                "report_sink",
            ],
            state_sizes_gb=default_state_sizes(12),
        )
        super().__init__(tenant_id, replay_window_compression, metadata, record_source)

    def _run_pipeline(self, event: Dict) -> Dict:
        # 1) raw_parser
        stage = dict(event)
        stage["clicked"] = int(stage.get("clicked", stage.get("label", 0)))
        stage["user_id"] = int(stage.get("user_id", 0) or 0)
        stage["campaign_id"] = int(stage.get("campaign_id", 0) or 0)

        # 2) bot_filter
        stage["is_bot"] = int(stage["user_id"] % 97 == 0)

        # 3) sessionizer
        stage["session_key"] = f"{stage.get('user_id', 'na')}:{stage.get('session_id', 'na')}"

        # 4) url_normalizer
        stage["normalized_page"] = str(stage.get("page", "/")).split("?")[0].lower()

        # 5) geo_enrichment
        stage["geo_region"] = ["na", "eu", "apac", "latam"][stage["user_id"] % 4]

        # 6) device_enrichment
        stage["device_type"] = ["mobile", "desktop", "tablet"][stage["user_id"] % 3]

        # 7) campaign_join
        stage["campaign_tier"] = "premium" if stage["campaign_id"] % 5 == 0 else "standard"

        # 8) ctr_feature_builder
        stage["ctr_feature"] = round((stage["clicked"] + 1.0) / ((stage["campaign_id"] % 100) + 2.0), 6)

        # 9) rolling_aggregation
        stage["rolling_click_score"] = round(stage["ctr_feature"] * (1.2 if stage["clicked"] else 0.8), 6)

        # 10) window_ranker
        stage["campaign_rank"] = int((stage["campaign_id"] % 1000) / 100) + 1

        # 11) report_formatter
        stage["report_window_seconds"] = max(1, int(60 / self.replay_window_compression))
        stage["agg_key"] = f"campaign-{stage['campaign_id']}"

        # 12) report_sink
        stage["analytics_label"] = "conversion" if stage["clicked"] else "impression"
        stage["operator_trace"] = [
            "raw_parser",
            "bot_filter",
            "sessionizer",
            "url_normalizer",
            "geo_enrichment",
            "device_enrichment",
            "campaign_join",
            "ctr_feature_builder",
            "rolling_aggregation",
            "window_ranker",
            "report_formatter",
            "report_sink",
        ]
        return stage
