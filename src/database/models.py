from sqlalchemy import Column, Integer, Numeric, Select, String, Text, TIMESTAMP, SmallInteger, Float, Date, case
from sqlalchemy import select, cast, func, distinct, CTE, and_, or_, union, true, Lateral, FromClause
from sqlalchemy.orm import aliased

from src.core import manager

from ._view import MaterializedView, View


class DataORM(manager.Base):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True)

    datetime = Column(TIMESTAMP(timezone=True), primary_key=True)
    T = Column(Numeric)
    # Po = Column(Numeric)
    # P = Column(Numeric)
    # Pa = Column(Numeric)
    # U = Column(SmallInteger)
    # DD = Column(String)
    # Ff = Column(SmallInteger)
    # ff10 = Column(SmallInteger)
    # ff3 = Column(SmallInteger)
    # N = Column(String)
    # WW = Column(Text)
    # W1 = Column(Text)
    # W2 = Column(Text)
    # Tn = Column(Float)
    # Tx = Column(Float)
    # Cl = Column(Text)
    # Nh = Column(Text)
    # H = Column(Text)
    # Cm = Column(Text)
    # Ch = Column(Text)
    # VV = Column(Text)
    # Td = Column(Float)
    # RRR = Column(Text)
    # tR = Column(Float)
    # E = Column(Text)
    # Tg = Column(Float)
    # E_ = Column("E'", Text)
    sss = Column(Text)

    def __repr__(self) -> str:
        return f"<DataORM({self.datetime})>"


class AVGTempDayView(View):
    '''
    SELECT 
        datetime::DATE as date, ROUND(AVG("T"), 2) as avg_temp
    FROM 
        data
    GROUP BY 
        date
    ORDER BY 
        date
    '''
    name: str = 'avg_temp_day'
    selectable = (
        select(
            cast(DataORM.datetime, Date).label('date'),
            func.ROUND(func.AVG(DataORM.T), 2).label('avg_temp')
        ).
        group_by('date').
        order_by('date')
    )


class AVGTempMonthView(View):
    '''
    SELECT 
        TO_CHAR(datetime, 'YYYY-MM') as date, AVG(data."T") 
    FROM 
        data
    GROUP BY 
        date
    ORDER BY 
        date
    '''
    name: str = 'avg_temp_month'
    selectable: Select = (select(
        func.TO_CHAR(DataORM.datetime, 'YYYY-MM').label('date'), 
        func.ROUND(
            func.AVG(DataORM.T), 2
            ).label('avg_temp')
            ).
        group_by('date').
        order_by('date'))


class SnowDayCoverView(View):
    '''
    SELECT 
        datetime::DATE AS date, 
        SUM(CASE 
			WHEN "sss" ~ '^[0-9]+$' THEN "sss"::INT 
			WHEN "sss" IN ('Менее 0.5', 'Снежный покров не постоянный.') THEN 0.1
			ELSE 0 END
		   ) AS snow_cover
    FROM 
        data
    GROUP BY 
        date
    ORDER BY 
        date
    '''
    name: str = 'snow_cover_day'
    selectable: Select = (select(
        cast(DataORM.datetime, Date).label('date'),
        func.sum(
            case(
                (DataORM.sss.op('~')('^[0-9]+$') == True,
                    cast(DataORM.sss, Integer)),
                (DataORM.sss.in_(('Менее 0.5', 'Снежный покров не постоянный.')),
                    0.1),
                else_=0
            )
        ).label('snow_cover')
        ).
        group_by('date').
        order_by('date'))


class SnowDayProYearView(View):
    '''
    SELECT 
        EXTRACT(YEAR from date)::INT as year, count(snow_cover) 
    FROM 
        snow_cover_day
    WHERE 
        snow_cover != 0
    GROUP BY 
        year
    ORDER BY 
        year
    '''
    name: str = 'snow_pro_year'
    _snow_days_aliased = aliased(SnowDayCoverView.table)
    selectable: Select = (select(
        func.EXTRACT('year', _snow_days_aliased.c.date).cast(Integer).label('years'),
        func.count(_snow_days_aliased.c.snow_cover).label('snow_days')).
        where(_snow_days_aliased.c.snow_cover != 0).
        group_by('years').
        order_by('years'))


class MaxSnowCoverSeason1MaterializedView(MaterializedView):
    '''
    with seasons as (
        SELECT DISTINCT EXTRACT(YEAR FROM date) - 1 as year_a, EXTRACT(YEAR FROM date) as year_b
        FROM snow_cover_day
		UNION
        SELECT DISTINCT EXTRACT(YEAR FROM date) as year_a, EXTRACT(YEAR FROM date) + 1 as year_b
        FROM snow_cover_day
        ORDER BY 
        year_a
    )

    SELECT 
        FORMAT('%s/%s', year_a, year_b) as season, 
        MAX(snow_cover) 
    FROM 
        seasons, 
        snow_cover_day
    WHERE
        EXTRACT(MONTH FROM date) >= 7 AND EXTRACT(YEAR FROM date) = year_a
        OR
        EXTRACT(MONTH FROM date) < 7 AND EXTRACT(YEAR FROM date) = year_b
    GROUP BY
        season
    ORDER BY
        season
    '''
    name: str = 'max_snow_cover_season_cte'
    _snow_covers_aliased = aliased(SnowDayCoverView.table)
    _cte: CTE = (union(
                    select(
                        distinct(func.extract('year', _snow_covers_aliased.c.date).op('-')(1)).label('year_a'), 
                        func.extract('year', _snow_covers_aliased.c.date).label('year_b'),
                        )
                        .select_from(_snow_covers_aliased),
                    select(
                        distinct(func.extract('year', _snow_covers_aliased.c.date)).label('year_a'), 
                        func.extract('year', _snow_covers_aliased.c.date).op('+')(1).label('year_b'),
                        )
                        .select_from(_snow_covers_aliased)
                    ).
                order_by('year_a')
            ).cte('seasons')
    selectable: Select = (select(
            func.format('%s/%s', _cte.c.year_a, _cte.c.year_b).label('season'),
            func.max(_snow_covers_aliased.c.snow_cover).label('max_snow_cover')).
            select_from(_cte, _snow_covers_aliased).
            where(
                or_(
                    and_(func.extract('month', _snow_covers_aliased.c.date) >= 7, func.extract('year' , _snow_covers_aliased.c.date) == _cte.c.year_a),
                    and_(func.extract('month', _snow_covers_aliased.c.date) < 7, func.extract('year', _snow_covers_aliased.c.date) == _cte.c.year_b)
                    )
                ).
            group_by('season').
            order_by('season')
            )


class MaxSnowCoverSeason2MaterializedView(MaterializedView):
    '''
    SELECT
        CASE 
            WHEN EXTRACT(MONTH FROM date) >= 7 
                THEN FORMAT('%s/%s', EXTRACT(YEAR FROM date)::INT, (EXTRACT(YEAR FROM date) + 1)::INT)
            ELSE 
                FORMAT('%s/%s', (EXTRACT(YEAR FROM date)::INT -1), EXTRACT(YEAR FROM date)::INT)
        END 
        AS season_start_year,
        MAX(snow_cover)
    FROM
        snow_cover_day
    GROUP BY season_start_year
    ORDER BY season_start_year
    '''
    name: str = 'max_snow_cover_season_case'
    _snow_covers_aliased = aliased(SnowDayCoverView.table)
    selectable: Select = (select(
        case(
            (func.extract('month', _snow_covers_aliased.c.date) >= 7,
            func.format('%s/%s', func.extract('year', _snow_covers_aliased.c.date), func.extract('year', _snow_covers_aliased.c.date).op('+')(1))),
            else_=func.format('%s/%s', func.extract('year', _snow_covers_aliased.c.date).op('-')(1), func.extract('year', _snow_covers_aliased.c.date))
        ).label('season'),
        func.max(_snow_covers_aliased.c.snow_cover).label('max_snow_cover')).
        group_by('season').
        order_by('season')
        )


class DaysWithSnowCoverSeasonMaterializedView(MaterializedView):
    '''    
    with seasons as (
        SELECT DISTINCT EXTRACT(YEAR FROM date) - 1 as year_a, EXTRACT(YEAR FROM date) as year_b
        FROM snow_cover_day
		UNION
        SELECT DISTINCT EXTRACT(YEAR FROM date) as year_a, EXTRACT(YEAR FROM date) + 1 as year_b
        FROM snow_cover_day
        ORDER BY 
        year_a
    )

    SELECT 
        FORMAT('%s/%s', year_a, year_b) as season, 
        COUNT(snow_cover) 
    FROM 
        seasons, 
        snow_cover_day
    WHERE
        (EXTRACT(MONTH FROM date) >= 7 AND EXTRACT(YEAR FROM date) = year_a
        	OR
        EXTRACT(MONTH FROM date) < 7 AND EXTRACT(YEAR FROM date) = year_b)
		AND 
		snow_cover != 0
		
    GROUP BY
        season
    ORDER BY
        season
    '''
    name: str = 'days_with_snow_season'
    _snow_covers_aliased = aliased(SnowDayCoverView.table)
    _cte: CTE = (union(
                    select(
                        distinct(func.extract('year', _snow_covers_aliased.c.date).op('-')(1)).label('year_a'), 
                        func.extract('year', _snow_covers_aliased.c.date).label('year_b'),
                        )
                        .select_from(_snow_covers_aliased),
                    select(
                        distinct(func.extract('year', _snow_covers_aliased.c.date)).label('year_a'), 
                        func.extract('year', _snow_covers_aliased.c.date).op('+')(1).label('year_b'),
                        )
                        .select_from(_snow_covers_aliased)
                    ).
                order_by('year_a')
            ).cte('seasons')
    selectable: Select = (select(
            func.format('%s/%s', _cte.c.year_a, _cte.c.year_b).label('season'),
            func.count(_snow_covers_aliased.c.snow_cover).label('max_snow_cover')).
            select_from(_cte, _snow_covers_aliased).
            where(
                and_(
                    or_(
                        and_(func.extract('month', _snow_covers_aliased.c.date) >= 7, func.extract('year' , _snow_covers_aliased.c.date) == _cte.c.year_a),
                        and_(func.extract('month', _snow_covers_aliased.c.date) < 7, func.extract('year', _snow_covers_aliased.c.date) == _cte.c.year_b)
                        ),
                    _snow_covers_aliased.c.snow_cover != 0
                    )
                ).
            group_by('season').
            order_by('season')
            )


class SnowFallLJMaterializedView(MaterializedView):
    '''
    with maxer as (SELECT
        CASE 
            WHEN EXTRACT(MONTH FROM date) >= 7 
                THEN EXTRACT(YEAR FROM date)::INT
            ELSE 
                EXTRACT(YEAR FROM date)::INT -1
        END 
        AS season_start_year,
        MAX(snow_cover) as mcmax
    FROM
        snow_cover_day
    GROUP BY season_start_year)

    SELECT 
        season_start_year, date, snow_cover
    FROM 
        maxer
    JOIN LATERAL
        (
            SELECT * FROM snow_cover_day
            WHERE 
                snow_cover = maxer.mcmax
                AND
                date 
                    BETWEEN 
                (season_start_year||'-07-01')::DATE 
                    AND 
                (season_start_year + 1||'-07-01')::DATE
            ORDER BY date DESC
            LIMIT 1
        ) as s
    ON TRUE
    ORDER BY season_start_year;
    '''
    name: str = 'snow_melt_lateral_join'
    _snow_covers_aliased: FromClause = aliased(SnowDayCoverView.table)
    _cte: CTE = (select(
            case(
                (func.extract('month', _snow_covers_aliased.c.date) >= 7,
                func.extract('year', _snow_covers_aliased.c.date)),
                else_=func.extract('year', _snow_covers_aliased.c.date).op('-')(1)
            ).label('season_start_year'),
            func.max(_snow_covers_aliased.c.snow_cover).label('mcmax')
        ).
        select_from(_snow_covers_aliased).
        group_by('season_start_year')
    ).cte('maxer')
    _lateral: Lateral = (select(
            _snow_covers_aliased.c.date, _snow_covers_aliased.c.snow_cover
        ).
        where(
            and_(
                _snow_covers_aliased.c.snow_cover == _cte.c.mcmax,
                _snow_covers_aliased.c.date.between(
                    (_cte.c.season_start_year.cast(Text) + '-07-01').cast(Date),
                    ((_cte.c.season_start_year.op('+')(1)).cast(Text) + '-07-01').cast(Date)
                )
            )
        ).
        select_from(_snow_covers_aliased).
        order_by(_snow_covers_aliased.c.date.desc()).
        limit(1)
    ).lateral()
    selectable: Select = (select(
            _cte.c.season_start_year, _lateral.c.date, _lateral.c.snow_cover
        ).
        select_from(_cte).
        join(_lateral, true()).
        order_by(_cte.c.season_start_year)
    )


class SnowFallWFView(View):
    '''
    with maxer as (SELECT
        CASE 
            WHEN EXTRACT(MONTH FROM date) >= 7 
                THEN EXTRACT(YEAR FROM date)::INT
            ELSE 
                EXTRACT(YEAR FROM date)::INT -1
        END 
        AS season_start_year,
        MAX(snow_cover) as mcmax
    FROM
        snow_cover_day
    GROUP BY season_start_year)


    SELECT 	season_start_year, date, mcmax FROM 
    (SELECT 
        season_start_year, 
        date, 
        mcmax,
        ROW_NUMBER() OVER (PARTITION BY season_start_year ORDER BY date DESC) as ron
    FROM 
        maxer, snow_cover_day
    WHERE
        snow_cover = mcmax
        AND
        date BETWEEN
            (season_start_year||'-07-01')::DATE
            AND
            (season_start_year + 1||'-07-01')::DATE

    ) as wf
    WHERE 
        ron = 1
    ;
    '''
    name: str = 'snow_melt_window_function'
    _snow_covers_aliased: FromClause = aliased(SnowDayCoverView.table)
    _cte: CTE = (select(
            case(
                (func.extract('month', _snow_covers_aliased.c.date) >= 7,
                func.extract('year', _snow_covers_aliased.c.date)),
                else_=func.extract('year', _snow_covers_aliased.c.date).op('-')(1)
            ).label('season_start_year'),
            func.max(_snow_covers_aliased.c.snow_cover).label('mcmax')
        ).
        select_from(_snow_covers_aliased).
        group_by('season_start_year')
    ).cte('maxer')
    _sub_query: Select = (select(
            _cte.c.season_start_year, _snow_covers_aliased.c.date, _cte.c.mcmax, func.row_number().over(
                partition_by=_cte.c.season_start_year,
                order_by=_snow_covers_aliased.c.date.desc()
            ).label('ron')).
            select_from(_cte, _snow_covers_aliased).
            where(
                and_(
                    _snow_covers_aliased.c.snow_cover == _cte.c.mcmax,
                    _snow_covers_aliased.c.date.between(
                        (_cte.c.season_start_year.cast(Text) + '-07-01').cast(Date),
                        ((_cte.c.season_start_year.op('+')(1)).cast(Text) + '-07-01').cast(Date)
                    )
                )
            ).subquery('wf')
            )
    selectable: Select = (select(
            _sub_query.c.season_start_year, _sub_query.c.date, _sub_query.c.mcmax).
            select_from(_sub_query).
            where(_sub_query.c.ron == 1)
            )
    