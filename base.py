from collections import deque
import threading
import sqlite3
import typing
import time


class database_base:
    INIT_STATEMENTS: str

    def __init__(self, path: str = '.sqlite') -> None:
        self.database = sqlite3.connect(path)
        self.database.execute(self.INIT_STATEMENTS)
        self.database.execute(
            'create table if not exists CHECKED_IDS (id integer primary key)',
        )

    def add_to_data(self, iden: int, data: dict | None) -> None:
        self.database.execute(
            f'insert or replace into CHECKED_IDS values ({iden})',
        )
        self.database.commit()

    def get_min(self) -> int | None:
        record = self.database.execute('''
            select id as I from CHECKED_IDS order by I asc limit 1
        ''').fetchone()
        return record[0] if record else None

    def get_max(self) -> int | None:
        record = self.database.execute(f'''
            select id as I from CHECKED_IDS order by I desc limit 1
        ''').fetchone()
        return record[0] if record else None

    def get_holes(self) -> list[tuple[int, int, int]]:
        return self.database.execute(f'''
            select prev_id, id, id - prev_id as diff from (
                select id, lag(id) over (order by id) as prev_id from checked_ids
            ) where id - prev_id > 1 order by diff asc
        ''').fetchall()

    def commit(self):
        self.database.commit()


class lambda_database(database_base):
    SCHEMA: dict[str, dict[str, dict[str, typing.Any]]]

    def __init__(self, path: str = '.sqlite') -> None:
        super().__init__(path)
        for table_name, table_fields in self.SCHEMA.items():
            params = ', '.join([
                f'{field} {item["type"]}'
                for i, (field, item) in enumerate(table_fields.items())
            ])
            self.database.execute(
                f'create table if not exists {table_name} ({params})',
            )

    @staticmethod
    def __do_lambda(func, iden: int, data: dict | None) -> list[str]:
        try:
            return func(iden, data)
        except Exception as _:
            return []

    def add_to_data(self, iden: int, data: dict | None) -> None:
        super().add_to_data(iden, data)
        if not data:
            return

        for table_name, table_fields in self.SCHEMA.items():
            fields_str = ', '.join(table_fields.keys())
            fill_ins = ', '.join(['?'] * len(table_fields))
            calls = [
                self.__do_lambda(item['func'], iden, data)
                for field, item in table_fields.items()
            ]
            lens = [
                len(c)
                for c in calls
            ]
            max_len = max(lens)
            calls = [
                [None] * max_len
                if l == 0 else
                c * (max_len // l)
                for l, c in zip(lens, calls)
            ]
            params = list(zip(*calls))
            self.database.executemany(
                f'insert or replace into {table_name} ({fields_str}) values ({fill_ins})', params,
            )
        self.database.commit()


class scraper_base:
    RANGE_MIN = 1
    RANGE_MAX = float('inf')

    @staticmethod
    def try_entry(iden: int) -> typing.Any | None:
        raise NotImplementedError()

    def __init__(
        self,
        database: database_base,
        iden_list: list[int],
        thread_count: int = 1,
    ) -> None:
        self.queue = deque[tuple[int, dict]]()
        self.database = database
        self.thread_count = 0
        self.quit = False
        self.limit = 0

        def __thread_body(gen: typing.Generator[tuple[int, dict], None, None]) -> None:
            self.thread_count += 1
            for o in gen:
                self.queue.append(o)
            self.thread_count -= 1

        self.thread_list = [
            threading.Thread(
                target=__thread_body,
                args=[
                    self.__process(iden_list[o::thread_count]),
                ],
            )
            for o in range(0, thread_count)
        ]

    def is_in_range(self, iden: int, entry) -> bool:
        return self.RANGE_MIN <= iden <= self.RANGE_MAX

    def __print_progress(self, i: int, iden: int, info) -> None:
        iden_str = f'{iden:10d}'
        if self.quit:
            print(f"{iden_str} ({i}/{self.limit} - {self.thread_count})\n", end='')
        elif not info:
            return
        else:
            print(iden_str)

    def __process(self, r: typing.Iterable[int]) -> typing.Generator[tuple[int, typing.Any], None, None]:
        for i, base_iden in enumerate(r):
            if i > self.limit:
                if self.quit:
                    break
                self.limit = i

            info = self.try_entry(base_iden)
            self.__print_progress(i, base_iden, info)
            yield (base_iden, info)

    def __join_threads(self) -> None:
        self.quit = True
        for t in self.thread_list:
            t.join()
            while len(self.queue) > 0:
                self.queue_pop()

    def queue_pop(self) -> None:
        i, e = self.queue.pop()
        self.database.add_to_data(i, e)

        if e and not self.is_in_range(i, e):
            self.__join_threads()
            return

    def run(self) -> None:
        for t in self.thread_list:
            t.start()

        try:
            while self.thread_count > 0:
                while len(self.queue) == 0:
                    time.sleep(0.01)
                self.queue_pop()

        except KeyboardInterrupt:
            print('Quitting program soon...')
            self.__join_threads()
