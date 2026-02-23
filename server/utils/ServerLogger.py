import logging

logger = logging.getLogger("uvicorn.error")

class ServerLogger:

    boot = "🥾"
    spark = "⚡"
    success = "✅"
    pallette = "🎨"
    fire = "🔥"
    bug = "🐛"
    hotfix = "🚑"
    feature = "✨"
    doc = "📝"
    deploy = "🚀"
    WIP = "🚧"
    drunk = "🍻"
    party = "🎉"
    python = "🐍"
    verbose = "🔊"
    confusion = "🌀"
    accurate = "📘"
    drama = "🎭"
    docs = "📚"
    danger="🛑"

    def info(self, message: str, emoji: str = ""):
        logger.info(f"{emoji} {message}")
    
    def warn(self, message: str, emoji: str = ""):
        logger.warning(f"{emoji} {message}")

    def error(self, message: str, emoji: str = ""):
        logger.error(f"{emoji} {message}")

    def critical(self, message: str, emoji: str = ""):
        logger.critical(f"{emoji} {message}")

    def deb(self, message: str, emoji: str = ""):
        logger.debug(f"{emoji} {message}")
