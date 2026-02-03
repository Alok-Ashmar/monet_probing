import logging

logger = logging.getLogger("uvicorn.error")

class ServerLogger:

    boot = "🥾"
    spark = "⚡"
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

    def info(self, message: str):
        logger.info(message)

    def warn(self, message: str):
        logger.warning(message)

    def err(self, message: str):
        logger.error(message)

    def critical(self, message: str):
        logger.critical(message)

    def deb(self, message: str):
        logger.debug(message)
