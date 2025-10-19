import os
import shutil
import asyncio
from pathlib import Path
from pyrogram.filters import command
from pyrogram.handlers import MessageHandler

from bot import LOGGER, bot
from bot.helper.ext_utils.bot_utils import new_task, cmd_exec
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.message_utils import send_message, edit_message, send_file, delete_message


@new_task
async def github_clone_handler(_, message):
    msg = await send_message(message, "⏳ Processing GitHub repository...")
    
    cmd = message.text.split(maxsplit=1)
    if len(cmd) == 1:
        await edit_message(msg, "usage: <code>/github &lt;url&gt;</code>")
        return
    
    url = cmd[1].strip()
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'
    if "github.com" not in url:
        await edit_message(msg, "❌ Invalid GitHub URL!")
        return
    
    try:
        repo_name = url.rstrip('/').split('/')[-1].replace('.git', '')
        if not repo_name:
            raise ValueError("Invalid repo name")
    except Exception:
        await edit_message(msg, "❌ Could not extract repository name from URL!")
        return
    
    user_id = message.from_user.id
    base_dir = Path("downloads").resolve()
    base_dir.mkdir(parents=True, exist_ok=True)
    
    temp_dir = base_dir / f"{user_id}_{repo_name}"
    zip_path = base_dir / f"{repo_name}.zip"
    
    try:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        if zip_path.exists():
            os.remove(zip_path)
        
        temp_dir.mkdir(parents=True, exist_ok=True)
        await edit_message(msg, f"📥 Cloning repository: <code>{repo_name}</code>...")
        
        clone_cmd = f"git clone --depth 1 '{url}' '{temp_dir}'"
        stdout, stderr, returncode = await cmd_exec(clone_cmd)
        
        if returncode != 0:
            error_msg = stderr if stderr else "Unknown error"
            if "Authentication failed" in error_msg or "could not read" in error_msg:
                await edit_message(msg, "❌ Authentication failed! Check your access token.")
            elif "Repository not found" in error_msg:
                await edit_message(msg, "❌ Repository not found or you don't have access!")
            else:
                await edit_message(msg, f"❌ Clone failed:\n<code>{error_msg[:500]}</code>")
            return
        
        git_dir = temp_dir / ".git"
        if git_dir.exists():
            shutil.rmtree(git_dir)
        
        await edit_message(msg, f"📦 Creating archive: <code>{repo_name}.zip</code>...")    
        shutil.make_archive(str(zip_path.with_suffix('')), 'zip', str(temp_dir))
        
        if not zip_path.exists():
            await edit_message(msg, "❌ Failed to create zip archive!")
            return
    
        file_size = zip_path.stat().st_size
        size_mb = file_size / (1024 * 1024)
        
        await edit_message(msg, f"📤 Uploading <code>{repo_name}.zip</code> ({size_mb:.2f} MB)...")
        await send_file(message, zip_path, caption=f"📦 <b>{repo_name}</b>\n💾 Size: {size_mb:.2f} MB")
        await delete_message(msg)
        
    except asyncio.TimeoutError:
        await edit_message(msg, "❌ Operation timed out! Repository might be too large.")
    except Exception as e:
        LOGGER.error(f"GitHub clone error: {e}")
        await edit_message(msg, f"❌ Error: <code>{str(e)[:500]}</code>")
    finally:
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            if zip_path.exists():
                os.remove(zip_path)
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")


bot.add_handler(
    MessageHandler(
        github_clone_handler,
        filters=command("github") & CustomFilters.authorized
    )
)
