import psycopg2
import traceback
import testHelper
import Interface as MyAssignment

DATABASE_NAME = "ratingsdb"
RATINGS_TABLE = "ratings"
INPUT_FILE_PATH = "test_data.dat"
ACTUAL_ROWS_IN_INPUT_FILE = 20
PARTITIONS = 5

if __name__ == "__main__":
    try:
        testHelper.createdb(DATABASE_NAME)
        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            testHelper.deleteAllPublicTables(conn)

            result, e = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            print("loadratings:", "pass" if result else f"fail ({e})")

            result, e = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, PARTITIONS, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            print("rangepartition:", "pass" if result else f"fail ({e})")

            result, e = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3.0, conn, "2")
            print("rangeinsert:", "pass" if result else f"fail ({e})")

            testHelper.deleteAllPublicTables(conn)
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            result, e = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, PARTITIONS, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            print("roundrobinpartition:", "pass" if result else f"fail ({e})")

            result, e = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3.0, conn, "4")
            print("roundrobininsert:", "pass" if result else f"fail ({e})")

            input("Nhấn Enter để xóa tất cả các bảng...")
            testHelper.deleteAllPublicTables(conn)

    except Exception as detail:
        traceback.print_exc()
