"""
chris_notification_channel.py
==============================
Wraps the existing ChRIS `Notification` class (chris_notification.py) as a
NotificationChannel, so pipeline code stops calling run_notification_plugin
directly and instead just relies on NotificationManager routing.

Per-run values (plugin instance ID, recipients) don't live on the channel -
they're passed in through NotificationContext.extra, since they differ per
pipeline run, not per app-wide config.
"""

from loguru import logger

from notifications import NotificationChannel, NotificationContext, NotificationEvent
from chris_notification import Notification


class ChRISNotificationChannel(NotificationChannel):
    """
    App-wide config (CUBE url/token, SMTP server) lives here.
    Per-run config (plugin instance id, recipients) comes from ctx.extra.
    """
    name = "chris"

    def __init__(self, cube_url: str, cube_token: str, smtp_server: str):
        self.client = Notification(cube_url, cube_token)
        self.smtp_server = smtp_server

    def send(self, ctx: NotificationContext) -> None:
        pv_id = ctx.extra.get("plugin_instance_id")
        recipients = ctx.extra.get("recipients")

        if pv_id is None or not recipients:
            logger.warning(
                "ChRISNotificationChannel: missing plugin_instance_id/recipients "
                f"in ctx.extra for event={ctx.event.value}; skipping."
            )
            return

        if ctx.event == NotificationEvent.ERROR:
            # Build error-specific email
            self.client.run_notification_plugin(
                pv_id=pv_id,
                msg=f"Pipeline error: {ctx.message}",
                rcpts=recipients,
                smtp=self.smtp_server,
                search_data="",
            )
        else:
            # Success path
            self.client.run_notification_plugin(
                pv_id=pv_id,
                msg=ctx.message,
                rcpts=recipients,
                smtp=self.smtp_server,
                search_data="",
            )