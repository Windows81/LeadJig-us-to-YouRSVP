import sqlite3
import base
import requests

VOWELS = ['A', 'E', 'U']
CONSONANTS = ['B', 'C', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z']

PARTS = [
    {
        'choices': VOWELS,
        'char_index': 4,
    },
    {
        'choices': VOWELS,
        'char_index': 1,
    },
    {
        'choices': CONSONANTS,
        'char_index': 0,
    },
    {
        'choices': CONSONANTS,
        'char_index': 2,
    },
    {
        'choices': CONSONANTS,
        'char_index': 3,
    },
    {
        'choices': CONSONANTS,
        'char_index': 5,
    },
]

CHOICE_COUNTS = [len(p['choices']) for p in PARTS]
MAX_IDEN = 1
for c in CHOICE_COUNTS:
    MAX_IDEN *= c


class leadjig_database(base.lambda_database):
    INIT_STATEMENTS = """
        create view if not exists JOINED as select * from CAMPAIGNS natural join EVENTS natural join LURES natural join ADVISORS where length(lures.code) = 6;
    """
    SCHEMA = {
        'CAMPAIGNS': {
            'mapped_id': {
                'func': lambda iden, data: [iden],
                'type': 'integer primary key',
            },
            'campaign_id': {
                'func': lambda iden, data: [data['campaign']['id']],
                'type': 'string',
            },
            'name': {
                'func': lambda iden, data: [data['campaign']['name']],
                'type': 'string',
            },
            'headline': {
                'func': lambda iden, data: [data['campaign']['headline']],
                'type': 'string',
            },
            'sub_headline': {
                'func': lambda iden, data: [data['campaign']['sub_headline']],
                'type': 'string',
            },
            'description': {
                'func': lambda iden, data: [data['campaign']['description']],
                'type': 'string',
            },
            'redirect_url': {
                'func': lambda iden, data: [data['campaign']['redirect_url']],
                'type': 'string',
            },
            'video_link': {
                'func': lambda iden, data: [data['campaign']['video_link']],
                'type': 'string',
            },
            'advisor_id': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['id']
                ],
                'type': 'string',
            },
        },
        'EVENTS': {
            'event_id': {
                'func': lambda iden, data: [
                    e['id'] for e in data['campaign']['events']
                ],
                'type': 'string primary key',
            },
            'title': {
                'func': lambda iden, data: [
                    e['title'] for e in data['campaign']['events']
                ],
                'type': 'string',
            },
            'venue': {
                'func': lambda iden, data: [
                    e['venue'] for e in data['campaign']['events']
                ],
                'type': 'string',
            },
            'address_full': {
                'func': lambda iden, data: [
                    e['address_full'] for e in data['campaign']['events']
                ],
                'type': 'string',
            },
            'geo_lat': {
                'func': lambda iden, data: [
                    e['address_coordinates'][0] for e in data['campaign']['events']
                ],
                'type': 'real',
            },
            'geo_lon': {
                'func': lambda iden, data: [
                    e['address_coordinates'][1] for e in data['campaign']['events']
                ],
                'type': 'real',
            },
            'start_time': {
                'func': lambda iden, data: [
                    # Event timestamps are local time; misleading to include `Z`.
                    e['start_time'].rstrip('Z') for e in data['campaign']['events']
                ],
                'type': 'string',
            },
            'webinar_key': {
                'func': lambda iden, data: [
                    e['webinar_key'] for e in data['campaign']['events']
                ],
                'type': 'string',
            },
            'campaign_id': {
                'func': lambda iden, data: [
                    data['campaign']['id']
                ],
                'type': 'string',
            },
        },
        'LURES': {
            'code': {
                'func': lambda iden, data: [
                    e['code'] for e in data['campaign']['lures']
                ],
                'type': 'string primary key',
            },
            'lure_id': {
                'func': lambda iden, data: [
                    e['id'] for e in data['campaign']['lures']
                ],
                'type': 'string',
            },
            'channel': {
                'func': lambda iden, data: [
                    e['channel'] for e in data['campaign']['lures']
                ],
                'type': 'string',
            },
            'campaign_id': {
                'func': lambda iden, data: [
                    data['campaign']['id']
                ],
                'type': 'string',
            },
        },
        'ADVISORS': {
            'advisor_id': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['id']
                ],
                'type': 'string primary key',
            },
            'full_name': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['full_name']
                ],
                'type': 'string',
            },
            'company_logo': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['company_logo']
                ],
                'type': 'string',
            },
            'company_email': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['company_email']
                ],
                'type': 'string',
            },
            'company_phone': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['company_phone']
                ],
                'type': 'string',
            },
            'company_phone_extension': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['company_phone_extension']
                ],
                'type': 'string',
            },
            'company_website': {
                'func': lambda iden, data: [
                    data['campaign']['advisor']['company_website']
                ],
                'type': 'string',
            },
            'mapped_id': {
                'func': lambda iden, data: [iden] * len(data['campaign']['events']),
                'type': 'integer',
            },
        },
    }


class leadjig_scraper(base.scraper_base):
    RANGE_MIN = 0
    RANGE_MAX = MAX_IDEN - 1
    DEFAULT_THREAD_COUNT = 3

    @staticmethod
    def _convert_id(iden: int) -> str | None:
        arr = len(PARTS) * ['']
        for p in reversed(PARTS):
            l = len(p['choices'])
            index = iden % l
            iden //= l
            arr[p['char_index']] = p['choices'][index]

        if iden > 0:
            return None
        return ''.join(arr)

    @staticmethod
    def try_entry(iden: int):
        iden_str = leadjig_scraper._convert_id(iden)
        while True:
            try:
                res = requests.get(f'https://a9ssvdmczd.execute-api.us-east-1.amazonaws.com/production/campaigns/{iden_str}')
                break
            except requests.exceptions.ConnectionError:
                pass
        if res.status_code == 404 or res.status_code >= 500:
            return None
        elif res.status_code == 200:
            return res.json()
        raise requests.ConnectionError(f'Unable to pull from {iden_str}')
