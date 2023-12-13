from datetime import datetime, timezone, timedelta
from typing import List, Callable, Dict, FrozenSet

import pycountry
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import select, tuple_
from sqlalchemy.sql.schema import Table, Column, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Integer, String, DateTime

from podx.batch.db.funcs.core import get_dma_name_by_id
import podx.db.base as db_base
import podx.db.enums.sqltypes as db_enums_types
from podx.batch.db.manager import db_manager
from podx.batch.db.mapping import TIER_INTERVAL_TABLE, TIER_OVERRIDE_TABLE, TIER_ID_NAME
from podx.batch.logging import get_logger
from podx.batch.util.aws.s3 import object_exists, parse_s3_url, iter_bucket_objects, iter_s3_gzip_tsv
from podx.batch.util.itertools import grouper
from podx.common.const.metrics import IntervalMetric, Category, MAPREDUCED_METRICS
from podx.common.const.vendors import Vendor, MAPREDUCED_VENDOR_DIRS
from podx.common.util.metrics import interval_metric_to_components
from .db import get_internal_id_by_provider

logger = get_logger(__name__)

DELTA_DAY = timedelta(days=1)


def _convert_dma(dma_code):
    return get_dma_name_by_id(dma_code)


def _convert_country(country_code):
    country = pycountry.countries.get(alpha_2=country_code)
    if not country:
        return
    return country.name


MAPREDUCE_VALUE_CONVERTERS: Dict[IntervalMetric, Callable] = {
    IntervalMetric.EPISODE_DAY_CITY: _convert_dma,
    IntervalMetric.PODCAST_DAY_CITY: _convert_dma,
    IntervalMetric.EPISODE_DAY_COUNTRY: _convert_country,
    IntervalMetric.PODCAST_DAY_COUNTRY: _convert_country
}


def create_staging_table(metric_type: IntervalMetric, metric_date: datetime) -> Table:
    engine = db_manager.get_engine()
    table_name = f"temp_{metric_type.name.lower()}_{metric_date.strftime('%Y%m%d')}"
    logger.info(f'Creating staging table {table_name}')
    table = Table(
        table_name, db_base.metadata,
        Column('internal_id', UUID()),
        Column('metrics_ts', DateTime(True)),
        Column('metrics_category_cd', db_enums_types.METRICS_CATEGORY_TYPE),
        Column('metrics_value', String(100)),
        Column('metrics_count', Integer, nullable=False, default=0),
        PrimaryKeyConstraint('internal_id', 'metrics_ts', 'metrics_category_cd', 'metrics_value'),
        prefixes=['TEMPORARY']
    )
    table.create(engine)
    return table


def stage_metrics(bucket: str, vendor: Vendor, interval_metric: IntervalMetric,
                  metric_date: datetime, staging_table: Table):
    vendor_dirs = MAPREDUCED_VENDOR_DIRS[vendor]
    for vendor_dir in vendor_dirs:
        base_results_path = 'results/{}/daily'.format(vendor_dir)
        for stage in ['stage2', 'stage1']:
            s3_url = 's3://{bucket}/{base}/{date}/metrics/{stage}/{metric_dir}/'.format(
                bucket=bucket,
                base=base_results_path,
                date=metric_date.strftime('%Y/%m/%d'),
                stage=stage,
                metric_dir=interval_metric.name.lower())
            if object_exists(s3_url + '_SUCCESS'):
                logger.info('Found {}'.format(s3_url))
                stage_metrics_path(vendor, interval_metric, s3_url,
                                   staging_table, metric_date)
                break


def stage_metrics_path(vendor: Vendor, metric_type: IntervalMetric,
                       s3_url: str, staging_table: Table, metric_date: datetime):
    bucket, path = parse_s3_url(s3_url)
    filecount = 0
    for obj in iter_bucket_objects(bucket, path, ('.bz2', '.gz')):
        stage_metrics_file(vendor, metric_type, bucket, obj['Key'],
                           staging_table, metric_date)
        filecount += 1
    if not filecount:
        raise Exception('No files processed')


def stage_metrics_file(vendor: Vendor, metric_type: IntervalMetric,
                       bucket: str, object_key: str, staging_table: Table,
                       metric_date: datetime):
    tier, interval, category = interval_metric_to_components(metric_type)

    logger.info('Loading metrics from file %s/%s', bucket, object_key)
    reader = iter_s3_gzip_tsv(bucket, object_key)
    counter = 0

    metrics_category_cd = category.name if category else 'DOWNLOADS'
    metrics_value = 'total'  # default if no category
    metrics_ts_prefix = metric_date.strftime('%Y-%m-%d')

    # For performance, grab 500 records at a time
    for group in grouper(500, reader):
        metrics = []
        for record in group:
            if category is not Category.DOWNLOADS:
                vendor_id, metrics_ts, metrics_value, metrics_count = record
            else:
                vendor_id, metrics_ts, metrics_count = record

            if not metrics_ts.startswith(metrics_ts_prefix):
                continue

            internal_id = get_internal_id_by_provider(vendor, tier, vendor_id)
            if not internal_id:
                # We're going to ignore this error. We should be fetching
                # podcasts and episodes from the vendor's APIs before this
                # job runs. Historically, when an episode or podcast couldn't
                # be found, it was because it was created as a test.
                logger.error(f'No ID found for {tier.name} {vendor_id}')
                continue

            try:
                convert_value: Callable = MAPREDUCE_VALUE_CONVERTERS[metric_type]
                metrics_value = convert_value(metrics_value)
            except KeyError:
                pass

            if not metrics_value:
                continue

            metrics.append({
                'internal_id': internal_id,
                'metrics_ts': metrics_ts,
                'metrics_category_cd': metrics_category_cd,
                'metrics_value': metrics_value,
                'metrics_count': metrics_count,
            })
            counter += 1
        if not metrics:
            continue
        increment_staged_metrics(metrics, staging_table)

    logger.info('Inserted %d metrics', counter)


def increment_staged_metrics(metrics: List[dict], staging_table: Table):
    engine = db_manager.get_engine()
    stmt = insert(staging_table)
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=[staging_table.c.internal_id, staging_table.c.metrics_ts,
                        staging_table.c.metrics_category_cd, staging_table.c.metrics_value],
        set_=dict(metrics_count=stmt.excluded.metrics_count + staging_table.c.metrics_count))
    engine.execute(on_update_stmt, metrics)


def upsert_staged_metrics(metrics: List[dict], staging_table: Table):
    engine = db_manager.get_engine()
    stmt = insert(staging_table)
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=[staging_table.c.internal_id, staging_table.c.metrics_ts,
                        staging_table.c.metrics_category_cd, staging_table.c.metrics_value],
        set_=dict(metrics_count=stmt.excluded.metrics_count))
    engine.execute(on_update_stmt, metrics)


def copy_staged_metrics(metric_type: IntervalMetric, staging_table: Table):
    tier, interval, category = interval_metric_to_components(metric_type)

    model_class = TIER_INTERVAL_TABLE[(tier, interval)]
    table = model_class.__table__
    now_dt = datetime.now(timezone.utc)
    engine = db_manager.get_engine()

    tier_id_name = TIER_ID_NAME[tier]
    stmt = insert(table)
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=[getattr(table.c, tier_id_name), table.c.metrics_ts,
                        table.c.metrics_category_cd, table.c.metrics_value],
        set_=dict(metrics_count=stmt.excluded.metrics_count,
                  updated_at=now_dt))

    total = 0
    for rows in paginate_staging_table(staging_table):
        insert_rows = [{
            tier_id_name: row[0],
            'metrics_ts': row[1],
            'metrics_category_cd': row[2],
            'metrics_value': row[3],
            'metrics_count': row[4],
            'created_at': now_dt
        } for row in rows]
        engine.execute(on_update_stmt, insert_rows)
        total += len(insert_rows)
        logger.info('Copied {} rows from {}'.format(len(insert_rows), staging_table.name))
    logger.info('Copied {} total rows from {}'.format(total, staging_table.name))


def paginate_staging_table(staging_table):
    engine = db_manager.get_engine()
    last_row = None
    while True:
        stmt = select([staging_table])
        if last_row:
            stmt = stmt.where(
                tuple_(staging_table.c.internal_id, staging_table.c.metrics_ts, staging_table.c.metrics_value)
                > tuple_(last_row[0], last_row[1], last_row[3]))
        stmt = stmt.order_by('internal_id', 'metrics_ts', 'metrics_value').limit(500)
        result = engine.execute(stmt)
        rows = result.fetchall()
        if not rows:
            break

        serialized = [(
            row.internal_id,
            row.metrics_ts,
            row.metrics_category_cd,
            row.metrics_value,
            row.metrics_count
        ) for row in rows]
        last_row = serialized[-1]
        yield serialized


def drop_staging_table(table: Table):
    logger.info('Dropping staging table {}'.format(table.name))
    engine = db_manager.get_engine()
    table.drop(engine)
    db_base.metadata.remove(table)


def get_mapreduced_metric_types(excluded: List[IntervalMetric] = None,
                                included: List[IntervalMetric] = None) -> FrozenSet[IntervalMetric]:
    if excluded is None:
        excluded = []
    if included is None:
        included = []
    excluded_types: FrozenSet[IntervalMetric] = frozenset(excluded)
    included_types: FrozenSet[IntervalMetric] = frozenset(included)
    return (MAPREDUCED_METRICS - excluded_types) | included_types


def stage_override_metrics(metric_type: IntervalMetric,
                           staging_table: Table,
                           metric_date: datetime):
    tier, interval, category = interval_metric_to_components(metric_type)
    model = TIER_OVERRIDE_TABLE[tier]
    id_column = TIER_ID_NAME[tier]
    metrics_category_cd = category.name if category else 'DOWNLOADS'
    session = db_manager.get_session()
    stmt = (session.query(model)
            .filter(model.metrics_interval == interval.name)
            .filter(model.metrics_ts == metric_date)
            .filter(model.metrics_category_cd == metrics_category_cd))
    rows = stmt.all()
    if not rows:
        return

    metrics = [{
        'internal_id': getattr(r, id_column),
        'metrics_ts': r.metrics_ts,
        'metrics_category_cd': r.metrics_category_cd,
        'metrics_value': r.metrics_value,
        'metrics_count': r.metrics_count,
    } for r in rows]

    logger.info('Overriding {} metrics'.format(len(metrics)))
    upsert_staged_metrics(metrics, staging_table)


def load_single_day_metrics(
        bucket: str,
        metric_date: datetime,
        exclude_types: List[IntervalMetric] = None,
        include_types: List[IntervalMetric] = None):

    logger.info('Loading metrics for {}'.format(metric_date))
    metric_types: FrozenSet[IntervalMetric] = get_mapreduced_metric_types(
        exclude_types, include_types)
    for metric_type in metric_types:
        staging_table = create_staging_table(metric_type, metric_date)
        for vendor in MAPREDUCED_VENDOR_DIRS.keys():
            stage_metrics(bucket, vendor, metric_type,
                          metric_date, staging_table)
        stage_override_metrics(metric_type, staging_table, metric_date)
        copy_staged_metrics(metric_type, staging_table)
        drop_staging_table(staging_table)


def load_multi_day_metrics(
        bucket: str,
        metric_dates: List[datetime],
        exclude_types: List[IntervalMetric] = None,
        include_types: List[IntervalMetric] = None):
    metric_dates = sorted(frozenset(metric_dates))
    for metric_date in metric_dates:
        load_single_day_metrics(bucket, metric_date, exclude_types, include_types)
