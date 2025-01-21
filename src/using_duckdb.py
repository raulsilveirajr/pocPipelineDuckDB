import duckdb 
import time

def count_duckdb():
    duckdb.sql(
        """
        SELECT count(*) AS total
        FROM read_csv("data/medicoes_10000000.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3,1)'})
    """
    ).show()


def create_duckdb():
    duckdb.sql(
        """
        SELECT station,
            MIN(temperature) AS min_temperature,
            CAST(AVG(temperature) AS DECIMAL(3,1)) AS mean_temperature,
            MAX(temperature) AS max_temperature
        FROM read_csv("data/medicoes_10000000.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3,1)'})
        GROUP BY station
        ORDER BY station
    """
    ).show(max_rows=10000)


if __name__ == "__main__":
    # import time
    start_time = time.time()
    count_duckdb()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")
