import functools

import podx.db.tables as db_tables
from podx.batch.db.manager import db_manager
from podx.common.const.metrics import Tier
from podx.common.const.vendors import Vendor
from podx.batch.const.metadata import MetadataType


@functools.lru_cache(maxsize=512)
def get_podcast_id_by_provider(provider: Vendor, vendor_podcast_id) -> str:
    db_session = db_manager.get_session()
    query = (db_session.query(db_tables.DataProviderMap.internal_id)
             .filter(db_tables.DataProviderMap.provider_code == provider.name)
             .filter(db_tables.DataProviderMap.provider_id == vendor_podcast_id)
             .filter(db_tables.DataProviderMap.data_category == MetadataType.PODCAST.name))
    return query.scalar()


@functools.lru_cache()
def get_episode_id_by_provider(vendor: Vendor, vendor_episode_id) -> str:
    db_session = db_manager.get_session()
    query = (db_session.query(db_tables.DataProviderMap.internal_id)
             .filter(db_tables.DataProviderMap.provider_code == vendor.name)
             .filter(db_tables.DataProviderMap.provider_id == vendor_episode_id)
             .filter(db_tables.DataProviderMap.data_category == MetadataType.EPISODE.name))
    return query.scalar()


def get_internal_id_by_provider(provider: Vendor, tier: Tier, provider_id) -> str:
    if tier is Tier.EPISODE:
        return get_episode_id_by_provider(provider, provider_id)
    elif tier is Tier.PODCAST:
        return get_podcast_id_by_provider(provider, provider_id)
    else:
        raise Exception(f'Unsupported tier passed: {tier.name}')
