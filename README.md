
### **THÔNG TIN**
- **Môn học:** Cơ sở dữ liệu phân tán
- **Nhóm:** 20
- **Thành viên nhóm:**
  - Lê Hải Đăng         - B22DCCN207: Báo cáo, Load Ratings 
  - Hoàng Bình Minh - B22DCCN530: Range Partition, Range Insert
  - Đậu Ngọc Nghĩa  - B22DCCN602: Roundrobin Partition, Roundrobin Insert

---

## **1. TỔNG QUAN**

### **1.1. Giới thiệu**
Trong thời đại ngày nay, việc lưu trữ và xử lý dữ liệu trở nên ngày càng phức tạp do sự gia tăng nhanh chóng của khối lượng thông tin. Chính vì vậy, các hệ quản trị cơ sở dữ liệu phân tán (DDBMS) ngày càng được ứng dụng rộng rãi để đáp ứng nhu cầu này. Một trong những thành phần quan trọng quyết định hiệu quả hoạt động của DDBMS là **phân mảnh dữ liệu**. Kỹ thuật này cho phép chia nhỏ cơ sở dữ liệu thành nhiều phần nhỏ hơn, giúp tối ưu hóa hiệu suất truy vấn, giảm tải hệ thống và tăng tính sẵn sàng của dữ liệu.

### **1.2. Khái niệm về phân mảnh**
Phân mảnh dữ liệu là kỹ thuật chia nhỏ một cơ sở dữ liệu thành nhiều đoạn, mỗi đoạn được lưu trữ tại một vị trí khác nhau trong hệ thống phân tán. Mục tiêu chính của phân mảnh là để cải thiện hiệu suất, tính mở rộng và khả năng quản lý dữ liệu. Thay vì lưu toàn bộ cơ sở dữ liệu tại một địa điểm duy nhất, các phần dữ liệu được phân tán theo cách hợp lý, phù hợp với nhu cầu sử dụng và vị trí địa lý của người dùng.

### **1.3. Các loại phân mảnh**
- **Phân mảnh ngang:** Phân mảnh ngang thực hiện việc chia các dòng dữ liệu (bản ghi) thành các nhóm riêng biệt. Mỗi phân mảnh chứa một tập hợp con các bản ghi trong bảng, thường dựa trên điều kiện thuộc tính. Ví dụ, phân tách khách hàng theo khu vực địa lý, mỗi khu vực lưu trữ dữ liệu của khách hàng tại đó. Điều này giúp truy vấn dữ liệu nhanh hơn đối với những truy vấn có điều kiện theo vùng.
- **Phân mảnh dọc:** Phân mảnh dọc tách bảng dữ liệu thành các phân mảnh chứa các cột khác nhau. Mỗi phân mảnh sẽ lưu trữ một số thuộc tính (cột) của bảng chính. Thông thường, khóa chính được lặp lại ở các phân mảnh để đảm bảo việc ghép dữ liệu về sau. Phân mảnh này phù hợp khi các ứng dụng chỉ quan tâm đến một số cột nhất định.
- **Phân mảnh kết hợp:** Phân mảnh kết hợp là sự pha trộn giữa phân mảnh ngang và dọc. Dữ liệu trước tiên có thể được phân mảnh ngang dựa trên giá trị của một thuộc tính, sau đó tiếp tục phân mảnh dọc tùy vào nhu cầu xử lý. Phân mảnh kết hợp thường được dùng trong các hệ thống phức tạp yêu cầu tối ưu hiệu năng cao.

### Trong phân mảnh ngang thì có 2 kỹ thuật là:
##### Range Partitioning
**Nguyên lý:** Phân chia dữ liệu dựa trên khoảng giá trị của một thuộc tính.

**Công thức:**
```
range_size = (max_value - min_value) / number_of_partitions
partition_index = floor((value - min_value) / range_size)
```

**Ưu điểm:**
- Hiệu quả cho range queries
- Dễ hiểu và triển khai
- Phù hợp với dữ liệu có phân phối đều

**Nhược điểm:**
- Có thể gây mất cân bằng nếu dữ liệu phân phối không đều
- Hot spots khi một khoảng giá trị được truy cập nhiều
##### Round Robin Partitioning
**Nguyên lý:** Phân phối dữ liệu đều vào các partition theo thứ tự vòng tròn.

**Công thức:**
```
partition_index = record_number % number_of_partitions
```

**Ưu điểm:**
- Đảm bảo phân phối đều dữ liệu
- Tránh hot spots
- Đơn giản và hiệu quả

**Nhược điểm:**
- Không hiệu quả cho range queries
- Khó khăn khi cần truy vấn theo điều kiện cụ thể
### **1.4. Tiêu chí đánh giá phân mảnh**
Một phương pháp phân mảnh hiệu quả cần đảm bảo một số tiêu chí quan trọng như:
- **Tính đầy đủ**: Tập hợp các phân mảnh phải bao phủ toàn bộ dữ liệu gốc, không thiếu sót.
- **Không trùng lặp**: Dữ liệu không được xuất hiện nhiều lần ở các phân mảnh khác nhau (trừ khóa chính nếu cần).
- **Khả năng tái lập**: Phải có khả năng khôi phục lại bảng gốc từ các phân mảnh khi cần thiết.
- **Tối ưu hóa hiệu suất**: Giảm thiểu thời gian xử lý và chi phí truyền tải dữ liệu trong hệ thống phân tán.

---

## **2. CẤU TRÚC DỮ LIỆU**

#### **Bảng chính (Master Table)**
```sql
CREATE TABLE ratings (
    userid INTEGER,
    movieid INTEGER,
    rating FLOAT
);
```

#### **Bảng phân mảnh**
**Range Partitions:**
```sql
CREATE TABLE range_part0 (userid INTEGER, movieid INTEGER, rating FLOAT);
CREATE TABLE range_part1 (userid INTEGER, movieid INTEGER, rating FLOAT);
-- ... range_part(n-1)
```

**Round Robin Partitions:**
```sql
CREATE TABLE rrobin_part0 (userid INTEGER, movieid INTEGER, rating FLOAT);
CREATE TABLE rrobin_part1 (userid INTEGER, movieid INTEGER, rating FLOAT);
-- ... rrobin_part(n-1)
```

---

## **3. THUẬT TOÁN**
#### **Prerequisites**
```bash
# Install Python 3.12+
# Install PostgreSQL 12+
pip install psycopg2
```
### **Load Ratings**
```python 
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
```
Hàm `loadratings()` thực hiện **tạo mới** bảng lưu trữ dữ liệu đánh giá phim từ một file dữ liệu đầu vào.

##### **Hoạt động chính:**

- **Kiểm tra file**: Xác minh file dữ liệu đầu vào có tồn tại không.
- **Tạo bảng mới**: Xóa bảng cũ (nếu có) và tạo mới bảng `ratingstablename` với 3 cột: `userid`, `movieid`, `rating`.
- **Đọc dữ liệu**: Đọc file dữ liệu, xử lý từng dòng, tách các trường dựa trên ký tự phân cách `::` và chuẩn hóa về dạng `userid \t movieid \t rating`.
- **Chèn dữ liệu**: Sử dụng `COPY` để đẩy toàn bộ dữ liệu vào bảng một cách hiệu quả.
- **Giao dịch an toàn**: Có rollback khi gặp lỗi đảm bảo tính toàn vẹn dữ liệu.
### **Range Partitioning Algorithm**
```python
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
```

Hàm `rangepartition()` thực hiện phân mảnh ngang bảng `ratingstablename` theo phương pháp **Range Partitioning**, dựa trên giá trị của cột `rating`. Bảng dữ liệu gốc sẽ được chia thành nhiều bảng nhỏ, mỗi bảng lưu trữ một khoảng giá trị rating riêng biệt.

##### **Hoạt động chính:**

- **Tính toán khoảng phân mảnh**: Vì `rating` nằm trong khoảng từ **0** đến **5**, hàm sẽ chia đều thành `numberofpartitions` đoạn.
- **Tạo bảng mới**: Với mỗi phân mảnh, hàm xóa bảng cũ (nếu có) rồi tạo bảng mới với tên dạng `RangePartX`.
- **Chèn dữ liệu**: Dữ liệu từ bảng gốc được lọc và chèn vào bảng phân mảnh tương ứng dựa trên khoảng rating đã tính toán.
- **Giao dịch an toàn**: Nếu có lỗi trong quá trình thực hiện, tất cả thay đổi sẽ bị hủy bỏ (`rollback`) để đảm bảo an toàn dữ liệu.

Hàm đảm bảo chia nhỏ dữ liệu thành các phân đoạn hợp lý, thuận tiện cho việc xử lý trong hệ thống phân mảnh cơ sở dữ liệu.

### **Round Robin Partitioning Algorithm**
```python
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
```

Hàm `roundrobinpartition()` thực hiện phân mảnh ngang bảng `ratingstablename` bằng phương pháp **Round Robin Partitioning**. Phương pháp này chia các dòng dữ liệu vào các bảng phân mảnh theo thứ tự lần lượt, lặp lại tuần tự qua từng bảng.

##### **Hoạt động chính:**

- **Tạo bảng phân mảnh**: Hàm tạo mới `numberofpartitions` bảng, đặt tên theo dạng `rrobin_partX`. Nếu bảng đã tồn tại, sẽ xóa và tạo lại.
- **Đánh số thứ tự**: Toàn bộ dữ liệu từ bảng gốc được đánh số dòng lần lượt bằng `ROW_NUMBER() OVER ()`.
- **Phân phối dữ liệu**: Các dòng dữ liệu được phân bổ lần lượt vào từng bảng theo nguyên tắc:
	`(số thứ tự dòng - 1) % numberofpartitions`
- **Giao dịch an toàn**: Nếu xảy ra lỗi, toàn bộ thao tác sẽ bị hoàn tác (`rollback`), đảm bảo dữ liệu không bị lỗi.

Phương pháp này đảm bảo dữ liệu được phân phối đồng đều về số lượng giữa các bảng phân mảnh.
### **Range Insert**
```python
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
```
Hàm `rangeinsert()` thực hiện chèn một bản ghi mới vào **cả bảng gốc** và **bảng phân mảnh** tương ứng trong phương pháp **Range Partitioning**.

##### **Hoạt động chính:**

- **Chèn vào bảng gốc**: Bản ghi mới `(userid, movieid, rating)` được thêm vào bảng gốc `ratingstablename`.
- **Xác định phân mảnh phù hợp**:
    - Đếm số bảng phân mảnh hiện tại.
    - Tính kích thước khoảng (`step`) mỗi phân mảnh dựa trên số lượng phân mảnh.
    - Xác định chỉ số phân mảnh phù hợp (`idx`) dựa vào giá trị `rating`.
- **Chèn vào bảng phân mảnh**: Bản ghi cũng được thêm vào bảng phân mảnh tương ứng theo chỉ số `idx`.
- **Giao dịch an toàn**: Hoàn tác (`rollback`) nếu có lỗi.
### **Round Robin Insert**
```python
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cur = openconnection.cursor()

    try:

        cur.execute(f"""

        INSERT INTO {ratingstablename} (userid, movieid, rating)

        VALUES (%s, %s, %s)

        """, (userid, itemid, rating))

        cur.execute("""

            SELECT COUNT(*)

            FROM information_schema.tables

            WHERE table_name LIKE %s

        """, (RROBIN_TABLE_PREFIX + '%',))

        numberofpartitions = cur.fetchone()[0]

        if numberofpartitions > 0:

            total_partition_records = 0

            for i in range(numberofpartitions):

                cur.execute(f"SELECT COUNT(*) FROM {RROBIN_TABLE_PREFIX}{i}")

                total_partition_records += cur.fetchone()[0]

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
```
Hàm `roundrobininsert()` có chức năng chèn một bản ghi mới vào **cả bảng gốc** và **một bảng phân mảnh** theo phương pháp **Round Robin Partitioning**.
##### **Hoạt động chính:**

- **Chèn vào bảng gốc**: Thêm bản ghi `(userid, movieid, rating)` vào bảng `ratingstablename`.
- **Xác định số phân mảnh**: Đếm số bảng phân mảnh hiện có với tiền tố `RROBIN_TABLE_PREFIX`.
- **Tính toán phân mảnh phù hợp**:
    - Đếm tổng số bản ghi trong tất cả các phân mảnh.
    - Xác định phân mảnh cần chèn dựa vào phép chia lấy dư (`total_partition_records % numberofpartitions`).
- **Chèn vào phân mảnh**: Thêm bản ghi vào bảng phân mảnh tương ứng.
- **Giao dịch an toàn**: Có xử lý rollback khi xảy ra lỗi.

---

## **4. ĐÁNH GIÁ HIỆU NĂNG**

Với dataset hơn 10 triệu bản ghi từ ratings.dat. Performance của các hàm.
#### **1. Load Performance**
| Load Time | Memory Usage |
| --------- | ------------ |
| ~30-60s   | ~50MB        |

#### **2. Partitioning Performance**
| Method      | Dataset Size | Partition Time |
| ----------- | ------------ | -------------- |
| Range       | 10M          | ~45s           |
| Round Robin | 10M          | ~50s           |

#### **3. Insert Performance**
| Method | Single Insert Time | Bulk Insert (1000) |
|--------|-------------------|-------------------|
| Range Insert | ~5ms | ~3s |
| Round Robin Insert | ~10ms | ~7s |

## **5. TỔNG KẾT**
### **Ưu và nhược điểm của các phương pháp phân mảnh**

#### **Range Partition**

- **Ưu điểm:**
    
    - Dễ tổ chức dữ liệu theo khoảng giá trị → Tăng tốc truy vấn khi biết trước phạm vi cần tìm.
        
    - Thuận tiện cho dữ liệu có phân bố không đồng đều.
    
- **Nhược điểm:**
    
    - Nếu dữ liệu không phân bố đều, có thể gây mất cân bằng giữa các phân mảnh.
        
    - Không tối ưu cho các truy vấn tổng thể trên toàn bộ dữ liệu.
#### **Round Robin Partition**

- **Ưu điểm:**
    
    - Phân phối dữ liệu đồng đều giữa các phân mảnh → Tránh trường hợp 1 phân mảnh quá lớn.
        
    - Thích hợp với các truy vấn không phụ thuộc vào giá trị cụ thể của dữ liệu.
    
- **Nhược điểm:**
    
    - Không hỗ trợ tốt cho các truy vấn có điều kiện cụ thể → Phải kiểm tra tất cả phân mảnh.
        
    - Không thể tối ưu hóa theo phạm vi giá trị như Range.

---

### **Bài học rút ra**

- **Tùy từng bài toán** cần lựa chọn kỹ thuật phân mảnh phù hợp. Nếu truy vấn thường theo khoảng → ưu tiên **Range**; nếu dữ liệu cần phân phối đều → chọn **Round Robin**.
- Thao tác với CSDL phân mảnh phức tạp hơn CSDL thông thường → cần chú ý cách thiết kế hàm Insert, Delete, Select tương ứng.
- Thực hành giúp nắm rõ sự khác biệt giữa **tư duy logic** khi chia dữ liệu và **thực thi kỹ thuật** với SQL.

---

## **Tổng kết**

Qua bài thực hành, nhóm đã hiểu rõ cách thức **phân mảnh ngang** trong hệ thống cơ sở dữ liệu phân tán. Việc triển khai trực tiếp bằng Python kết hợp PostgreSQL giúp củng cố kiến thức lý thuyết và kỹ năng lập trình thực tế. Bài học quan trọng nhất là phải lựa chọn **chiến lược phân mảnh phù hợp với bài toán**, không áp dụng máy móc.