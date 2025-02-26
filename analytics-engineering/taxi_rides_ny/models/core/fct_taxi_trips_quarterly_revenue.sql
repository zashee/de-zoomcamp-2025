WITH qtly_rev AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        service_type,
        SUM(total_amount) AS quarterly_revenue
    FROM
        {{ ref('fact_trips') }}
    WHERE pickup_datetime >= '2019-01-01' AND pickup_datetime < '2021-01-01'
    GROUP BY
        year, quarter, service_type
)

SELECT
    year,
    quarter,
    service_type,
    quarterly_revenue,
    LAG(quarterly_revenue) OVER (
        PARTITION BY service_type, quarter
        ORDER BY year
    ) AS prev_year_quarterly_revenue,
    ROUND((quarterly_revenue - LAG(quarterly_revenue) OVER (
        PARTITION BY service_type, quarter
        ORDER BY year
    )) / NULLIF(LAG(quarterly_revenue) OVER (
        PARTITION BY service_type, quarter
        ORDER BY year
    ), 0) * 100, 2
    ) AS yoy_growth_percent
FROM
    qtly_rev