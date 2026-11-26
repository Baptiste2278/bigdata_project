"""Silver -> gold : agrégats Spark SQL en tables Iceberg, validation et export Parquet."""
import time

from common import EXPORT, banner, build_spark

# Distance orthodromique en km (formule de haversine)
HAVERSINE_KM = """ROUND(2 * 6371 * ASIN(SQRT(
    POWER(SIN(RADIANS(dst.latitude - src.latitude) / 2), 2)
    + COS(RADIANS(src.latitude)) * COS(RADIANS(dst.latitude))
    * POWER(SIN(RADIANS(dst.longitude - src.longitude) / 2), 2))))"""

GOLD = {
    # Q1 : connectivité de chaque aéroport (vols directs, routes valides uniquement)
    "airport_connectivity": """
        SELECT src.airport_id, src.iata, src.name, src.city, src.country,
               COUNT(*)                         AS routes_out,
               COUNT(DISTINCT r.dst_airport_id) AS destinations,
               COUNT(DISTINCT r.airline_code)   AS airlines,
               COUNT(DISTINCT dst.country)      AS countries_served,
               RANK() OVER (ORDER BY COUNT(DISTINCT r.dst_airport_id) DESC) AS destinations_rank
        FROM of.silver.routes AS r
        JOIN of.silver.airports AS src ON src.airport_id = r.src_airport_id
        JOIN of.silver.airports AS dst ON dst.airport_id = r.dst_airport_id
        WHERE r.is_valid AND r.stops = 0
        GROUP BY src.airport_id, src.iata, src.name, src.city, src.country""",
    # Q2 : pays atteignables en direct depuis Paris (CDG + ORY)
    "paris_reachability": f"""
        SELECT dst.country,
               COUNT(DISTINCT r.dst_airport_id) AS destinations,
               COUNT(*)                         AS routes,
               COUNT(DISTINCT r.airline_code)   AS airlines,
               MIN({HAVERSINE_KM})              AS min_distance_km
        FROM of.silver.routes AS r
        JOIN of.silver.airports AS src ON src.airport_id = r.src_airport_id
        JOIN of.silver.airports AS dst ON dst.airport_id = r.dst_airport_id
        WHERE r.is_valid AND r.stops = 0 AND src.iata IN ('CDG', 'ORY')
        GROUP BY dst.country""",
    # Q2 bis : part des pays du dataset accessibles depuis Paris
    "country_reachability": """
        SELECT p.countries_reachable, t.countries_total,
               ROUND(100.0 * p.countries_reachable / t.countries_total, 1) AS pct_reachable
        FROM (SELECT COUNT(*) AS countries_reachable FROM of.gold.paris_reachability) AS p
        CROSS JOIN (SELECT COUNT(DISTINCT country) AS countries_total FROM of.silver.airports) AS t""",
}


def scalar(spark, query):
    return spark.sql(query).collect()[0][0]


def validate(spark):
    silver_rows = scalar(spark, "SELECT COUNT(*) FROM of.silver.routes")
    assert silver_rows == 67663, silver_rows

    direct = scalar(spark, "SELECT COUNT(*) FROM of.silver.routes WHERE is_valid AND stops = 0")
    gold = scalar(spark, "SELECT SUM(routes_out) FROM of.gold.airport_connectivity")
    assert direct == gold, (direct, gold)

    paris_silver = scalar(spark, """
        SELECT COUNT(*) FROM of.silver.routes AS r
        JOIN of.silver.airports AS a ON a.airport_id = r.src_airport_id
        WHERE r.is_valid AND r.stops = 0 AND a.iata IN ('CDG', 'ORY')""")
    paris_gold = scalar(spark, "SELECT SUM(routes) FROM of.gold.paris_reachability")
    assert paris_silver == paris_gold, (paris_silver, paris_gold)
    banner(f"validation OK silver={silver_rows} direct_valid={direct} paris_routes={paris_gold}")


def main():
    t0 = time.perf_counter()
    banner("silver_to_gold start")
    spark = build_spark("openflights-silver-to-gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS of.gold")

    for name, query in GOLD.items():  # l'ordre compte : country_reachability lit paris_reachability
        t = time.perf_counter()
        spark.sql(f"CREATE OR REPLACE TABLE of.gold.{name} USING iceberg AS {query}")
        banner(f"of.gold.{name} written in {time.perf_counter() - t:.1f}s")

    validate(spark)
    spark.sql("""
        SELECT iata, city, country, destinations, routes_out, airlines, destinations_rank
        FROM of.gold.airport_connectivity
        ORDER BY destinations_rank
        LIMIT 10""").show(truncate=False)
    spark.table("of.gold.country_reachability").show()

    for name in GOLD:
        (spark.table(f"of.gold.{name}").coalesce(1)
         .write.mode("overwrite").parquet(f"{EXPORT}/gold/{name}"))
    banner(f"silver_to_gold done in {time.perf_counter() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
