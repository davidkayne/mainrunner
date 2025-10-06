import re
import httpx
from bs4 import BeautifulSoup
from pyrogram.filters import command
from pyrogram.handlers import MessageHandler, EditedMessageHandler

from bot import LOGGER, bot
from bot.helper.ext_utils.bot_utils import new_task
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.message_utils import send_message, edit_message

"""
Extract JAV codes from torrent titles for the specific single page
"""

JAV_CODE_PATTERN = re.compile(r'\b([A-Z]{2,6}-\d{2,5})\b', re.IGNORECASE)


@new_task
async def jav_code_handler(_, message):
    msg = await send_message(message, "⏳ Processing...")
    cmd = message.text.split(maxsplit=1)
    if len(cmd) == 1:
        await edit_message(msg, f"Usage: <code>/jdata nyaa_url</code>")
        return
    url = cmd[1]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

        codes = []
        for link in soup.select("tr td a"):
            title = link.text.strip()
            match = JAV_CODE_PATTERN.search(title)
            if match:
                codes.append(match.group(1).upper())
        codes = list(dict.fromkeys(codes))
        if not codes:
            await edit_message(msg, "No codes found on this page.")
        else:
            formatted = "\n".join(f"<code>{c}</code>" for c in codes)
            await edit_message(msg, formatted)

    except Exception as e:
        await edit_message(msg, f"Error: <code>{e}</code>")


bot.add_handler(
    MessageHandler(
        jav_code_handler, filters=command("jdata") & CustomFilters.owner
    )
)