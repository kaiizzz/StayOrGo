from typing import Optional

from slack.slack import Slack


class SlackUpdateMixin:
    def _send_slack_update(self, success: bool, exception: Optional[Exception] = None) -> None:
        if getattr(self, "slack_updates", False):
            try:
                Slack().send_app_update(
                    f"{'Backup ' if getattr(self, 'is_backup', False) else ''}{self.__class__.__name__} ({getattr(self, 'industry', '').capitalize()})",
                    success,
                    exception=exception
                )
            except Exception as e:
                if hasattr(self, "logger"):
                    self.logger.exception(f"Failed to send Slack update: {e}")
