import discord
from discord.ext import commands
import os
import aiohttp
import asyncio
import string
import logging
import copy
from datetime import datetime

from cogs.utils import checks
from cogs.utils.dataIO import fileIO
from cogs.utils.chat_formatting import *
from __main__ import send_cmd_help

try:
    import feedparser
except:
    feedparser = None

log = logging.getLogger("red.rss")


MAX_UPDATES = 10


class Settings(object):
    pass


class Feeds(object):
    def __init__(self):
        self.check_folders()

        # {
        #     URL: {
        #         server: {
        #             name: {
        #                 channel_id:,template:,
        #                 last_update:,posted:,update_time:
        #                 filtered_tag:,keyword:
        #             }
        #         }
        #     }
        # }
        self.feeds = fileIO("data/RSS/feeds.json", "load")

        # A map of { server: {name: url} } ,
        #  to be able to find what url the name belongs to within a server.
        self._reverse_map = {}
        self.reload_reverse_map()

    def reload_reverse_map(self):
        for url, server_feeds in self.feeds.items():
            for server_id, feeds in server_feeds.items():
                if server_id not in self._reverse_map:
                    self._reverse_map[server_id] = {}
                for name in feeds.keys():
                    self._reverse_map[server_id][name] = url

    def get_url_for_name(self, server_id, name):
        if server_id not in self._reverse_map:
            return None
        if name not in self._reverse_map[server_id]:
            return None
        return self._reverse_map[server_id][name]

    def save_feeds(self):
        fileIO("data/RSS/feeds.json", "save", self.feeds)

    def check_folders(self):
        if not os.path.exists("data/RSS"):
            print("Creating data/RSS folder...")
            os.makedirs("data/RSS")
        self.check_files()

    def check_files(self):
        f = "data/RSS/feeds.json"
        if not fileIO(f, "check"):
            print("Creating empty feeds.json...")
            fileIO(f, "save", {})

    def add_feed(self, server_id, channel_id, name, url, filtered_tag, keyword):
        if filtered_tag is None or keyword is None:
            filtered_tag = ""
            keyword = ""

        if url not in self.feeds:
            self.feeds[url] = {}
        if server_id not in self.feeds[url]:
            self.feeds[url][server_id] = {}

        new_feed = {
            'channel_id': channel_id,
            'template': "$name:\n$title",
            'mention': "",
            'filtered_tag': filtered_tag,
            'keyword': keyword,
            'last_update': "",
            'update_time': "",
            'posted': []
        }

        self.feeds[url][server_id][name] = new_feed
        self.save_feeds()

        if server_id not in self._reverse_map:
            self._reverse_map[server_id] = {}
        self._reverse_map[server_id][name] = url

    async def remove_feed(self, server_id, name):
        url = self.get_url_for_name(server_id, name)

        if url is None or url not in self.feeds:
            return False
        if server_id not in self.feeds[url]:
            return False
        if name not in self.feeds[url][server_id]:
            return False

        del self.feeds[url][server_id][name]
        del self._reverse_map[server_id][name]

        self.save_feeds()
        return True

    def update_feed(self, server_id, name, latest_title: str, update_time: str):
        url = self.get_url_for_name(server_id, name)

        if url is not None and url in self.feeds:
            if server_id in self.feeds[url]:
                if name in self.feeds[url][server_id]:
                    feed = self.feeds[url][server_id][name]

                    if feed['update_time'] != update_time:
                        feed['posted'] = []
                    feed['last_update'] = latest_title
                    feed['update_time'] = update_time

                    self.feeds[url][server_id][name] = feed
                    self.save_feeds()

    def posted(self, server_id, name, title):
        url = self.get_url_for_name(server_id, name)
        if url is not None and url in self.feeds:
            if name in self.feeds[url]:
                self.feeds[url][name]['posted'].append(title)
                self.save_feeds()

    async def edit_template(self, server_id, name, template):
        url = self.get_url_for_name(server_id, name)

        if url is not None and url in self.feeds:
            if server_id in self.feeds[url]:
                if name in self.feeds[url][server_id]:
                    feed = self.feeds[url][server_id][name]

                    if "<>" in template and feed['mention'] != "":
                        template = template.replace("<>", feed['mention'])
                    feed['template'] = template

                    self.save_feeds()
                    return True
        return False

    async def edit_mention(self, server_id, name, role):
        url = self.get_url_for_name(server_id, name)

        if url is not None and url in self.feeds:
            if server_id in self.feeds[url]:
                if name in self.feeds[url][server_id]:
                    feed = self.feeds[url][server_id][name]

                    feed['mention'] = role.mention

                    template = feed['template']
                    if "<>" in template:
                        feed['template'] = template\
                            .replace("<>", feed['mention'])

                    self.save_feeds()
                    return True
        return False

    def get_feed_names(self, server):
        server_id = server.id if isinstance(server, discord.Server) else server
        ret = []

        for url, feeds in self.feeds.items():
            if server_id not in feeds:
                continue
            for name in feeds[server_id].keys():
                    ret.append(name)
        return ret

    def get_copy(self):
        return self.feeds.copy()

    @staticmethod
    async def get_feed_at(url):
        text = None
        try:
            with aiohttp.ClientSession() as session:
                with aiohttp.Timeout(3):
                    async with session.get(url) as r:
                        text = await r.text()
        except:
            pass
        return text

    @staticmethod
    async def valid_url(url):
        text = await Feeds.get_feed_at(url)
        rss = feedparser.parse(text)

        return not bool(rss.bozo)

    @staticmethod
    def rss_time_from(time_str: str):
        return datetime.strptime(time_str, '%a, %d %b %Y %H:%M:%S %z')

    @staticmethod
    def rss_time_to_str(time: datetime):
        return time.strftime('%a, %d %b %Y %H:%M:%S %z')


class RSS(object):
    def __init__(self, bot):
        self.bot = bot

        self.settings = Settings()
        self.feeds = Feeds()
        self.session = aiohttp.ClientSession()

    def __unload(self):
        self.session.close()

    def get_channel_object(self, channel_id):
        channel = self.bot.get_channel(channel_id)
        if channel and \
                channel.permissions_for(channel.server.me).send_messages:
            return channel
        return None

    @commands.group(pass_context=True)
    @checks.mod_or_permissions(administrator=True)
    async def rss(self, ctx):
        """ RSS feed stuff. """
        if ctx.invoked_subcommand is None:
            await send_cmd_help(ctx)

    @rss.command(pass_context=True, name="add")
    async def _rss_add(self, ctx, name: str, url: str, filtered=None,
                       keyword: str = ""):
        """ Add an RSS feed to the current channel.
            You can provide a `keyword` to filter the feed by the `filtered`
            tag's content.
        """

        if filtered is not None and keyword == "":
            await self.bot.say("If a tag to filter is provided, you must also"
                               "provide a keyword to check for.")
            return

        server = ctx.message.server
        channel = ctx.message.channel

        if server is None:
            await self.bot.say("Command cannot be executed "
                               "through direct messages.")
            return

        valid_url = await Feeds.valid_url(url)
        if not valid_url:
            await self.bot.send_message(
                channel,
                'Invalid or unavailable URL.')
            return

        self.feeds.add_feed(
            server.id, channel.id, name, url,
            filtered, keyword.replace('_', ' ')
        )

        post_time, title = await self.get_last_entry(url)

        self.feeds.update_feed(server.id, name, title, post_time)

        await self.bot.say(
            'Feed "{}" added. Modify the template using'
            ' rss template'.format(name)
        )

    @rss.command(pass_context=True, name="remove")
    async def _rss_remove(self, ctx, name: str):
        """Removes a feed from this server"""
        server = ctx.message.server
        if server is None:
            await self.bot.say("Command cannot be executed "
                               "through direct messages.")
            return

        success = await self.feeds.remove_feed(server.id, name)

        await self.bot.say('Feed deleted.' if success else 'Feed not found!')

    @rss.command(pass_context=True, name="list")
    async def _rss_list(self, ctx):
        """List currently running feeds"""

        server = ctx.message.server
        if server is None:
            await self.bot.say("Command cannot be executed "
                               "through direct messages.")
            return

        feed_names = self.feeds.get_feed_names(server)

        if len(feed_names) == 0:
            await self.bot.say("No feeds found for this server.")
            return

        await self.bot.say(box(
            "Available Feeds:\n\t" +
            "\n\t".join(feed_names)
        ))

    @rss.command(pass_context=True, name="notify")
    async def _rss_notify(self, ctx, feed_name: str, role: discord.Role):
        """ Sets the role which the posts should tag. To specify where the
            notification should go within the template, add `<>` to it.
            E.g.: $name: <>, new update! $title.
        """
        server = ctx.message.server
        if server is None:
            await self.bot.say("Command cannot be executed "
                               "through direct messages.")
            return

        success = await self.feeds.edit_mention(
            server.id, feed_name, role
        )
        if success:
            await self.bot.say("Role mention modified successfully.")
        else:
            await self.bot.say('Feed not found!')



    @rss.command(pass_context=True, name="template")
    async def _rss_template(self, ctx, feed_name: str, *, template: str):
        ("""Set a template for the feed alert

        Each variable must start with $, valid variables:
        \tauthor, author_detail, comments, content, contributors, created,"""
         """ create, link, name, published, published_parsed, publisher,"""
         """ publisher_detail, source, summary, summary_detail, tags, title,"""
         """ title_detail, updated, updated_parsed""")

        server = ctx.message.server
        if server is None:
            await self.bot.say("Command cannot be executed "
                               "through direct messages.")
            return

        template = template.replace("\\t", "\t")
        template = template.replace("\\n", "\n")
        success = await self.feeds.edit_template(
            server.id, feed_name, template
        )
        if success:
            await self.bot.say("Template modified successfully.")
        else:
            await self.bot.say('Feed not found!')

    @rss.command(pass_context=True, name="force")
    async def _rss_force(self, ctx, feed_name: str):
        """ Forces a feed alert."""
        channel = ctx.message.channel
        feeds = self.feeds.get_copy()

        server = ctx.message.server
        if server is None:
            await self.bot.say("Command cannot be executed "
                               "through direct messages.")
            return

        url = self.feeds.get_url_for_name(server.id, feed_name)

        if url is None or url not in feeds:
            await self.bot.say("Feed not found.")
            return
        if server.id not in feeds[url]:
            await self.bot.say("Feed not found.")
            return
        if feed_name not in feeds[url][server.id]:
            await self.bot.say("There are no feeds for with this name.")
            return

        items = copy.deepcopy(feeds[url][server.id][feed_name])

        result = await self.post_feed_updates(
            server.id, channel, feed_name,
            items, await self.get_feed_entries(url)
        )

        if not result:
            message = "No new entries found."
            if items['filtered_tag'] != "":
                message += (" Current filter: \"{}\" on `{}` tag"
                            .format(items['keyword'], items['filtered_tag']))

            await self.bot.say(message)

    async def post_feed_updates(self, server_id, channel, name, items, entries):
        log.debug("Posting updates for feed {}".format(name))

        template = items['template']

        if items['update_time'] == "":
            last_time = Feeds.rss_time_from(entries[0].published)
        else:
            last_time = Feeds.rss_time_from(items['update_time'])

        result = False
        last_idx = len(entries)
        for idx, entry in enumerate(entries):
            if last_time > Feeds.rss_time_from(entry.published):
                last_idx = idx
                break

        # Limit entries to be posted. Latest 10 by default.
        last_idx = min(MAX_UPDATES, last_idx)

        for entry in entries[:last_idx][::-1]:
            title = entry.title
            if title == items['last_update'] or title in items['posted']:
                continue

            if items['filtered_tag'] != "" and items['keyword'] not in \
                    getattr(entry, items['filtered_tag']):
                log.debug("Entry does not contain keyword {} in {}"
                          .format(items['keyword'], items['filtered_tag']))
                continue

            to_fill = string.Template(template)
            message = to_fill.safe_substitute(
                name=bold(name),
                **entry
            )

            if message is not None:
                await self.bot.send_message(channel, message)

                self.feeds.update_feed(server_id, name, title, entry.published)
                self.feeds.posted(server_id, name, title)

                result = True

        return result

    async def check_updates(self):
        await self.bot.wait_until_ready()

        while self == self.bot.get_cog('RSS'):
            all_feeds = self.feeds.get_copy()

            for url, feeds in all_feeds.items():
                rss_entries = await self.get_feed_entries(url)

                for server_id, server_feeds in feeds.items():

                    for name, items in server_feeds.items():
                        log.debug("Checking {} with URL {}".format(name, url))

                        channel = self.get_channel_object(items['channel_id'])

                        if channel is None:
                            log.debug("Response channel not found, continuing.")
                            continue

                        await self.post_feed_updates(
                            server_id, channel, name, items, rss_entries
                        )

            await asyncio.sleep(300)

    async def get_feed_entries(self, url):
        try:
            async with self.session.get(url) as resp:
                html = await resp.read()
        except:
            log.exception("Failure accessing feed at URL:\n\t{}".format(url))
            return None

        rss = feedparser.parse(html)

        if rss.bozo:
            log.debug("Feed at url below is bad.\n\t{}".format(url))
            return None

        if len(rss.entries) <= 0:
            log.debug("No entries found for feed at {}".format(url))
            return None

        return rss.entries

    async def get_last_entry(self, url):
        entries = await self.get_feed_entries(url)

        return entries[0].published, entries[0].title


def setup(bot):
    if feedparser is None:
        raise NameError("You need to run `pip3 install feedparser`")
    n = RSS(bot)
    bot.add_cog(n)
    bot.loop.create_task(n.check_updates())
