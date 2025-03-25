import asyncio
import logging
import os
import binascii
import aiohttp

import discord
from discord.ext import commands
import telegram
from telegram.ext import ApplicationBuilder, MessageHandler, filters, CommandHandler
from io import BytesIO
from PIL import Image
import io

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_GROUP_ID = os.environ.get("TELEGRAM_GROUP_ID")
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
DISCORD_CHANNEL_ID = int(os.environ.get("DISCORD_CHANNEL_ID"))

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Telegram Bot ---
async def telegram_forward(update, context):
    global TELEGRAM_GROUP_ID
    if TELEGRAM_GROUP_ID is None:
        return

    if update.effective_chat.id == int(TELEGRAM_GROUP_ID) and update.message:
        message = update.message
        text = message.text or message.caption or ""
        sender = message.from_user.first_name

        if message.photo:
            photo = message.photo[-1].file_id
            file = await context.bot.get_file(photo)
            file_bytes = await context.bot.get_file(message.photo[-1].file_id)
            file_data = await file_bytes.download_as_bytearray()
            try:
                image_stream = io.BytesIO(file_data)
                img = Image.open(image_stream)
                re_encoded_image = io.BytesIO()
                img.save(re_encoded_image, "JPEG")
                re_encoded_image.seek(0)
                await discord_channel.send(f"**{sender}:** {text}", file=discord.File(re_encoded_image, filename="photo.jpg"))
            except (ValueError, OSError) as e:
                logger.error(f"Error sending photo to Discord: {e}")
                await discord_channel.send(f"**{sender}:** Photo could not be sent. {text}")
        else:
            await discord_channel.send(f"**{sender}:** {text}")

async def get_chat_id(update, context):
    global TELEGRAM_GROUP_ID
    print("get_chat_id called") #add this line.
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Chat ID: {chat_id}")

    if TELEGRAM_GROUP_ID is None:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Please set the TELEGRAM_GROUP_ID environment variable to {chat_id} and restart the bot.")

async def telegram_error(update, context):
    logger.warning(f'Update {update} caused error {context.error}')

telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
telegram_app.add_handler(CommandHandler("chatid", get_chat_id))
telegram_app.add_handler(MessageHandler(filters.ALL, telegram_forward))
telegram_app.add_error_handler(telegram_error)

# --- Discord Bot ---
intents = discord.Intents.default()
intents.message_content = True
discord_bot = commands.Bot(command_prefix="!", intents=intents)

discord_channel = None

@discord_bot.event
async def on_ready():
    global discord_channel
    discord_channel = discord_bot.get_channel(DISCORD_CHANNEL_ID)
    if discord_channel is None:
        logger.error(f"Could not find Discord channel with ID {DISCORD_CHANNEL_ID}")
        await discord_bot.close()
        return

    logger.info(f"Discord bot connected as {discord_bot.user}")

@discord_bot.event
async def on_message(message):
    global TELEGRAM_GROUP_ID
    if message.channel.id == DISCORD_CHANNEL_ID and message.author != discord_bot.user and TELEGRAM_GROUP_ID is not None:

        if message.attachments:
            for attachment in message.attachments:
                if attachment.content_type.startswith("image/"): #Check if the attachment is an image.
                    async with aiohttp.ClientSession() as session:
                        async with session.get(attachment.url) as resp:
                            if resp.status == 200:
                                image_bytes = await resp.read()
                                await telegram_app.bot.send_photo(chat_id=int(TELEGRAM_GROUP_ID), photo=image_bytes, caption=f"**{message.author.name}:** {message.content}")
                            else:
                                logger.error(f"Failed to download image from Discord: {resp.status}")
                else:
                    await telegram_app.bot.send_message(chat_id=int(TELEGRAM_GROUP_ID), text=f"**{message.author.name}:** {message.content}")

        else:
            await telegram_app.bot.send_message(chat_id=int(TELEGRAM_GROUP_ID), text=f"**{message.author.name}:** {message.content}")

    await discord_bot.process_commands(message)

# --- Main Function ---
async def main():
    async with telegram_app:
        await telegram_app.start()
        discord_task = asyncio.create_task(discord_bot.start(DISCORD_BOT_TOKEN))
        await telegram_app.updater.start_polling()
        await discord_task
        await telegram_app.stop()

if __name__ == "__main__":
    asyncio.run(main())
