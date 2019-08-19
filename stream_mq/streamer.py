#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date : 2019-08-19
import asyncio
from json import loads, dumps
import aioredis


class Streamer(object):

    def __init__(self, channel: str, group_id: str, consume_topics: tuple, address: str, *args, **kwargs):
        self.channel = channel
        self.group_id = group_id
        self.redis_address = address
        self.position_args = args
        self.keyword_args = kwargs
        self.consume_topics = consume_topics
        self.consume_groups = []

        self.conn = None

    async def _make_connection(self):
        while not self.conn:
            try:
                self.conn = await aioredis.create_redis_pool(self.redis_address, *self.position_args,
                                                             **self.keyword_args)
            except Exception as e:
                self.conn = None
                print(e)
                print('redis reconnect')
            else:
                break

    async def get_conn(self):
        if not self.conn:
            await self._make_connection()
        return self.conn

    def _convert_message(self, message_body: dict):
        try:
            message_body = dumps(message_body)
        except Exception as e:
            print(e)
        return {
            'body': message_body
        }

    async def publish_message(self, message: dict, topic: str):
        converted_message = self._convert_message(message)
        conn = await self.get_conn()
        try:
            await conn.xadd(f'{self.channel}:{topic}', converted_message)
        except Exception as e:
            print(e)

    async def make_consume_group(self):
        await self.get_conn()
        self.consume_groups.clear()
        for topic in self.consume_topics:
            target_topic = f'{self.channel}:{topic}'
            try:
                await self.conn.xgroup_create(target_topic, self.group_id)
                self.consume_groups.append(topic)
            except Exception as e:
                print(e)

    async def wait_for_messages(self):
        if not self.consume_groups:
            await self.make_consume_group()
        if not self.consume_groups:
            print('No message to consume')
        try:
            target_topics = [f'{self.channel}:{topic}' for topic in self.consume_topics]
            while True:
                message = await self.conn.xread_group(self.group_id, self.group_id, target_topics, latest_ids=['>'])
                print('=' * 20)
                print('received message: {}'.format(message))
        except Exception as e:
            print(e)
