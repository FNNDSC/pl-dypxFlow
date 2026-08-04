"""
notifications.py
=================
A central, pluggable notification system for pipeline applications.

Core idea:
- Pipeline code never talks to Slack/email/webhooks directly.
- It just calls NotificationManager.notify(...) or uses the `track()`
  context manager around a step/pipeline.
- A single config maps event types -> channels, so behavior can change
  without touching pipeline code.
"""

from __future__ import annotations

import enum
import json
import logging
import time
import traceback
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Events
# --------------------------------------------------------------------------

class NotificationEvent(enum.Enum):
    START = "start"
    END = "end"
    SUCCESS = "success"
    ERROR = "error"


@dataclass
class NotificationContext:
    event: NotificationEvent
    pipeline_name: str
    step_name: Optional[str] = None
    message: str = ""
    error: Optional[BaseException] = None
    duration: Optional[float] = None
    extra: dict = field(default_factory=dict)


# --------------------------------------------------------------------------
# Channels (backends). Add new ones by subclassing NotificationChannel.
# --------------------------------------------------------------------------

class NotificationChannel:
    """Base class for a notification backend."""
    name = "base"

    def send(self, ctx: NotificationContext) -> None:
        raise NotImplementedError


class ConsoleChannel(NotificationChannel):
    name = "console"

    def send(self, ctx: NotificationContext) -> None:
        line = f"[{ctx.event.value.upper()}] {ctx.pipeline_name}"
        if ctx.step_name:
            line += f" / {ctx.step_name}"
        line += f" - {ctx.message}"
        print(line)


class EmailChannel(NotificationChannel):
    name = "email"

    def __init__(self, smtp_host, smtp_port, sender, recipients,
                 username=None, password=None, use_tls=True):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.sender = sender
        self.recipients = recipients
        self.username = username
        self.password = password
        self.use_tls = use_tls

    def send(self, ctx: NotificationContext) -> None:
        import smtplib
        from email.mime.text import MIMEText

        subject = f"[{ctx.event.value.upper()}] {ctx.pipeline_name}"
        body = ctx.message
        if ctx.error:
            body += "\n\n" + "".join(
                traceback.format_exception(type(ctx.error), ctx.error, ctx.error.__traceback__)
            )

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            if self.username:
                server.login(self.username, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())


class LegacySMTPAdapter(NotificationChannel):
    """
    Wraps an EXISTING smtp-notification module so you don't have to rewrite it.
    Point `send_fn` at whatever function your current module already exposes,
    e.g. `my_smtp_notifier.send_email(subject, body, to)`.

    This lets you migrate without touching the tested SMTP code at all -
    you're just changing *what decides to call it*.
    """
    name = "legacy_smtp"

    def __init__(self, send_fn, recipients):
        """
        send_fn: callable(subject: str, body: str, to: list[str]) -> None
                 (match this to your existing module's actual signature)
        recipients: default recipient list, or a dict keyed by event name
                    if different events should go to different people
        """
        self.send_fn = send_fn
        self.recipients = recipients

    def _resolve_recipients(self, ctx: NotificationContext):
        if isinstance(self.recipients, dict):
            return self.recipients.get(ctx.event.value, self.recipients.get("default", []))
        return self.recipients

    def send(self, ctx: NotificationContext) -> None:
        subject = f"[{ctx.event.value.upper()}] {ctx.pipeline_name}"
        body = ctx.message
        if ctx.error:
            body += "\n\n" + "".join(
                traceback.format_exception(type(ctx.error), ctx.error, ctx.error.__traceback__)
            )
        self.send_fn(subject, body, self._resolve_recipients(ctx))


class SlackChannel(NotificationChannel):
    name = "slack"

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, ctx: NotificationContext) -> None:
        text = f"*[{ctx.event.value.upper()}]* {ctx.pipeline_name}"
        if ctx.step_name:
            text += f" / {ctx.step_name}"
        text += f"\n{ctx.message}"
        if ctx.error:
            text += f"\n```{ctx.error}```"

        data = json.dumps({"text": text}).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url, data=data, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=5)


class WebhookChannel(NotificationChannel):
    """Generic JSON webhook, e.g. for PagerDuty, Discord, internal services."""
    name = "webhook"

    def __init__(self, url: str, headers: Optional[dict] = None):
        self.url = url
        self.headers = headers or {}

    def send(self, ctx: NotificationContext) -> None:
        payload = {
            "event": ctx.event.value,
            "pipeline": ctx.pipeline_name,
            "step": ctx.step_name,
            "message": ctx.message,
            "duration": ctx.duration,
            "error": str(ctx.error) if ctx.error else None,
        }
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json", **self.headers}
        req = urllib.request.Request(self.url, data=data, headers=headers)
        urllib.request.urlopen(req, timeout=5)


# --------------------------------------------------------------------------
# Central manager
# --------------------------------------------------------------------------

class NotificationManager:
    """
    Central registry mapping events -> channels.

    Usage:
        mgr = NotificationManager.instance()
        mgr.register_channel(ConsoleChannel())
        mgr.register_channel(SlackChannel(webhook_url="https://hooks.slack.com/..."))
        mgr.configure_routing(NotificationEvent.ERROR, ["console", "slack"])
        mgr.configure_routing(NotificationEvent.SUCCESS, ["console"])
    """

    _instance: Optional["NotificationManager"] = None

    def __init__(self):
        self.channels: dict[str, NotificationChannel] = {}
        self.routing: dict[NotificationEvent, list[str]] = {e: [] for e in NotificationEvent}
        self._hooks: list[Callable[[NotificationContext], None]] = []

    @classmethod
    def instance(cls) -> "NotificationManager":
        """Convenience singleton so pipeline code can grab the same manager anywhere."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register_channel(self, channel: NotificationChannel) -> "NotificationManager":
        self.channels[channel.name] = channel
        return self

    def configure_routing(self, event: NotificationEvent, channel_names: list[str]) -> "NotificationManager":
        self.routing[event] = channel_names
        return self

    def load_config(self, config: dict) -> "NotificationManager":
        """
        config = {
            "channels": {"console": ConsoleChannel(), "slack": SlackChannel(...)},
            "routing": {
                NotificationEvent.START: ["console"],
                NotificationEvent.ERROR: ["console", "slack"],
            },
        }
        """
        for name, channel in config.get("channels", {}).items():
            self.channels[name] = channel
        for event, names in config.get("routing", {}).items():
            self.routing[event] = names
        return self

    def load_routing_from_dict(self, routing: dict[str, list[str]]) -> "NotificationManager":
        """
        Load routing from plain strings (e.g. parsed from JSON/YAML), so this
        can live in a config file instead of code:

            {
              "start":   ["console"],
              "end":     ["console"],
              "success": ["console"],
              "error":   ["console", "legacy_smtp"]
            }

        Adding a new channel to an event, or a new event->channel mapping,
        now just means editing this file - no code change, no redeploy
        of pipeline logic.
        """
        for event_name, channel_names in routing.items():
            self.routing[NotificationEvent(event_name)] = channel_names
        return self

    def load_routing_from_file(self, path: str) -> "NotificationManager":
        import json
        with open(path) as f:
            data = json.load(f)
        return self.load_routing_from_dict(data.get("routing", data))

    def add_hook(self, fn: Callable[[NotificationContext], None]) -> "NotificationManager":
        """Optional: run arbitrary code on every event (metrics, logging, etc.)."""
        self._hooks.append(fn)
        return self

    def notify(self, ctx: NotificationContext) -> None:
        for fn in self._hooks:
            try:
                fn(ctx)
            except Exception:
                logger.exception("Notification hook failed")

        for name in self.routing.get(ctx.event, []):
            channel = self.channels.get(name)
            if not channel:
                logger.warning("Notification channel '%s' not registered", name)
                continue
            try:
                channel.send(ctx)
            except Exception:
                # Notification failures should never break the pipeline
                logger.exception("Failed to send notification via channel '%s'", name)

    @contextmanager
    def track(self, pipeline_name: str, step_name: Optional[str] = None, extra: Optional[dict] = None):
        """
        Wrap a pipeline or a single step:

            with NotificationManager.instance().track(
                "daily_ingest",
                extra={"plugin_instance_id": pv_id, "recipients": recipients},
            ):
                run_the_pipeline()

        Automatically fires START, then either SUCCESS or ERROR, then END.
        `extra` is per-run data (e.g. IDs, recipients) that channels need but
        that isn't part of app-wide config - it gets attached to every
        NotificationContext fired inside this `track()` block.
        """
        extra = extra or {}
        start_time = time.monotonic()
        self.notify(NotificationContext(
            event=NotificationEvent.START,
            pipeline_name=pipeline_name,
            step_name=step_name,
            message=f"Starting {step_name or pipeline_name}",
            extra=dict(extra),
        ))
        try:
            yield
        except Exception as exc:
            duration = time.monotonic() - start_time
            self.notify(NotificationContext(
                event=NotificationEvent.ERROR,
                pipeline_name=pipeline_name,
                step_name=step_name,
                message=f"Error in {step_name or pipeline_name}: {exc}",
                error=exc,
                duration=duration,
                extra=dict(extra),
            ))
            raise
        else:
            duration = time.monotonic() - start_time
            self.notify(NotificationContext(
                event=NotificationEvent.SUCCESS,
                pipeline_name=pipeline_name,
                step_name=step_name,
                message=f"{step_name or pipeline_name} completed successfully",
                duration=duration,
                extra=dict(extra),
            ))
        finally:
            duration = time.monotonic() - start_time
            self.notify(NotificationContext(
                event=NotificationEvent.END,
                pipeline_name=pipeline_name,
                step_name=step_name,
                message=f"Finished {step_name or pipeline_name}",
                duration=duration,
                extra=dict(extra),
            ))