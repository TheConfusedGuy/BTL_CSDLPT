# Interface.py – Tối ưu hóa, mô phỏng phân mảnh trên PostgreSQL
import psycopg2
import os
import io
from psycopg2 import sql

RANGE_TABLE_PREFIX = "range_part"
RROBIN_TABLE_PREFIX = "rrobin_part"

def getopenconnection(user='postgres', password='nghia', dbname='postgres', host='localhost', port=2209):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    if not os.path.exists(ratingsfilepath):
        raise FileNotFoundError(f"Không tìm thấy file: {ratingsfilepath}")
    cur = openconnection.cursor()
    try:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(ratingstablename)))
        cur.execute(sql.SQL("""
            CREATE TABLE {} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """).format(sql.Identifier(ratingstablename)))
        buf = io.StringIO()
        with open(ratingsfilepath, "r") as f:
            for line in f:
                parts = line.strip().split("::")
                if len(parts) >= 3:
                    buf.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
        buf.seek(0)
        cur.copy_expert(sql.SQL("""
            COPY {} (userid, movieid, rating)
            FROM STDIN WITH (FORMAT text, DELIMITER E'\t')
        """).format(sql.Identifier(ratingstablename)), buf)
        openconnection.commit()
    except Exception:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions < 1:
        raise ValueError("Số phân mảnh phải >= 1")
    cur = openconnection.cursor()
    try:
        step = 5.0 / numberofpartitions
        for i in range(numberofpartitions):
            tbl = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(tbl)))
            cur.execute(sql.SQL("""
                CREATE TABLE {} (userid INTEGER, movieid INTEGER, rating FLOAT)
            """).format(sql.Identifier(tbl)))
            low = i * step
            high = (i + 1) * step
            if i == 0:
                cond = f"rating >= {low} AND rating <= {high}"
            else:
                cond = f"rating > {low} AND rating <= {high}"
            cur.execute(sql.SQL("""
                INSERT INTO {} SELECT userid, movieid, rating FROM {}
                WHERE {}
            """).format(sql.Identifier(tbl), sql.Identifier(ratingstablename), sql.SQL(cond)))
        openconnection.commit()
    except Exception:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions < 1:
        raise ValueError("Số phân mảnh phải >= 1")
    cur = openconnection.cursor()
    try:
        for i in range(numberofpartitions):
            tbl = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(tbl)))
            cur.execute(sql.SQL("""
                CREATE TABLE {} (userid INTEGER, movieid INTEGER, rating FLOAT)
            """).format(sql.Identifier(tbl)))
        cur.execute(sql.SQL("""
            SELECT userid, movieid, rating, ROW_NUMBER() OVER () AS rnum FROM {}
        """).format(sql.Identifier(ratingstablename)))
        rows = cur.fetchall()
        for row in rows:
            idx = (row[3] - 1) % numberofpartitions
            cur.execute(sql.SQL("""
                INSERT INTO {} VALUES (%s, %s, %s)
            """).format(sql.Identifier(f"{RROBIN_TABLE_PREFIX}{idx}")), row[:3])
        openconnection.commit()
    except Exception:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cur = openconnection.cursor()
    try:
        cur.execute(sql.SQL("INSERT INTO {} VALUES (%s, %s, %s)").format(sql.Identifier(ratingstablename)), (userid, movieid, rating))
        cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE %s", (RANGE_TABLE_PREFIX + '%',))
        N = cur.fetchone()[0]
        step = 5.0 / N
        idx = int(rating / step)
        if rating % step == 0 and idx != 0:
            idx -= 1
        tbl = f"{RANGE_TABLE_PREFIX}{idx}"
        cur.execute(sql.SQL("INSERT INTO {} VALUES (%s, %s, %s)").format(sql.Identifier(tbl)), (userid, movieid, rating))
        openconnection.commit()
    except Exception:
        openconnection.rollback()
        raise
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm 5: Chèn record mới vào bảng gốc và phân mảnh Round Robin tiếp theo
    Fix logic để consistent với roundrobinpartition
    """
    cur = openconnection.cursor()
    
    try:
        # Insert vào bảng gốc trước
        cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Xác định số lượng partition
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name LIKE %s
        """, (RROBIN_TABLE_PREFIX + '%',))
        numberofpartitions = cur.fetchone()[0]
        
        if numberofpartitions > 0:
            # Logic: Tính partition dựa trên tổng số records trong tất cả partitions
            # để đảm bảo consistent với cách phân chia ban đầu
            total_partition_records = 0
            for i in range(numberofpartitions):
                cur.execute(f"SELECT COUNT(*) FROM {RROBIN_TABLE_PREFIX}{i}")
                total_partition_records += cur.fetchone()[0]
            
            # Partition index cho record mới
            index = total_partition_records % numberofpartitions
            table_name = f"{RROBIN_TABLE_PREFIX}{index}"
            
            cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
            """, (userid, itemid, rating))
        
        openconnection.commit()
        
    except Exception as e:
        openconnection.rollback()
        print(f"Lỗi khi chèn Round Robin Insert: {e}")
        raise
    finally:
        cur.close()
