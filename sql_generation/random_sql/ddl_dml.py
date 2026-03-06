import random
import re
import string
from datetime import datetime, timedelta
from typing import List, Optional

from data_structures.db_dialect import get_current_dialect
from sql_generation.random_sql.geometry import create_geometry_wkt
from data_structures.table import Table

def generate_create_table_sql(table: Table) -> str:
    """生成建表SQL语句"""
    # 获取当前数据库方言
    dialect = get_current_dialect()
    
    parts = [f"CREATE TABLE {table.name} ("]

    # 列定义
    column_defs = []
    for col in table.columns:
        is_primary_key = col.name == table.primary_key
        # 使用方言的方法生成列定义
        column_def = dialect.get_column_definition(
            col.name, 
            col.data_type, 
            col.is_nullable, 
            is_primary_key
        )
        column_defs.append(f"    {column_def}")

    # 如果没有使用方言的SERIAL PRIMARY KEY语法，则添加主键约束
    if not any("SERIAL" in def_ for def_ in column_defs) and table.primary_key:
        primary_key_def = dialect.get_primary_key_constraint(table.primary_key)
        column_defs.append(f"    {primary_key_def}")

    # 外键 - 仅在方言支持时添加
    if hasattr(dialect, 'supports_foreign_keys') and dialect.supports_foreign_keys():
        for fk in table.foreign_keys:
            column_defs.append(f"    FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']}({fk['ref_column']}) ")

    parts.append(",\n".join(column_defs))
    parts.append(");")

    return "\n".join(parts)

def generate_insert_sql(table: Table, num_rows: int = 5, existing_primary_keys: dict = None, primary_key_values: list = None) -> str:
    """生成插入SQL语句（每行一个INSERT）

    Args:
        table: 表对象
        num_rows: 生成的行数
        existing_primary_keys: 其他表的主键值字典，用于外键引用
    """
    if not table.columns:
        return ""

    # 获取当前数据库方言
    dialect = get_current_dialect()
    
    # 列名
    columns = [col.name for col in table.columns]

    # 如果没有提供主键值，则为非自增主键生成唯一值
    if primary_key_values is None:
        primary_key_values = set()
        for _ in range(num_rows):
            while True:
                val = random.randint(1, 10000)
                if val not in primary_key_values:
                    primary_key_values.add(val)
                    break
        primary_key_values = list(primary_key_values)

    # 生成单行INSERT语句
    insert_sqls = []
    for i in range(num_rows):
        row_values = []
        for col in table.columns:
            if col.name == table.primary_key:
                # 使用预先生成的唯一主键值
                row_values.append(str(primary_key_values[i]))
            elif any(fk for fk in table.foreign_keys if fk["column"] == col.name):
                # 处理外键，引用已存在的主键值
                fk = next(fk for fk in table.foreign_keys if fk["column"] == col.name)
                ref_table = fk["ref_table"]
                if existing_primary_keys and ref_table in existing_primary_keys and existing_primary_keys[ref_table]:
                    # 从引用表的主键值中随机选择一个
                    ref_pk_values = existing_primary_keys[ref_table]
                    if ref_pk_values:
                        selected_pk = random.choice(ref_pk_values)
                        row_values.append(str(selected_pk))
                    else:
                        # 如果引用表的主键值列表为空，使用随机值
                        row_values.append(str(random.randint(1, 100)))
                else:
                    # 如果没有引用表的主键值，使用随机值（实际应用中这可能会导致外键约束错误）
                    row_values.append(str(random.randint(1, 100)))
            # 根据具体数据类型决定生成逻辑
            elif col.data_type.startswith("INT") or col.data_type in ["BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT"]:
                if table.name == "orders" and col.name == "amount":
                    # orders表的amount列使用整数
                    row_values.append(str(random.randint(10, 1000)))
                else:
                    row_values.append(str(random.randint(0, 100)))
            elif col.data_type.startswith("DECIMAL") or col.data_type.startswith("NUMERIC"):
                row_values.append(f"{random.uniform(10, 1000):.2f}")
            elif col.data_type.startswith("FLOAT") or col.data_type.startswith("DOUBLE"):
                row_values.append(f"{random.uniform(0.0, 100.0):.2f}")
            elif col.data_type.startswith("VARCHAR") or col.data_type in ["CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT"]:
                # 对orders表的status列做特殊处理
                if table.name == "orders" and col.name == "status":
                    # 从三种状态中随机选择
                    status_values = ["finished", "finishing", "to_finish"]
                    row_values.append(f"'{random.choice(status_values)}'")
                # 对users表的email列做特殊处理，生成10位整数@qq.com格式
                elif table.name == "users" and col.name == "email":
                    # 生成10位随机整数
                    ten_digit_number = random.randint(1000000000, 9999999999)
                    row_values.append(f"'{ten_digit_number}@qq.com'")
                # 对users表的sex列做特殊处理，值为girl或boy
                elif table.name == "users" and col.name == "sex":
                    # 从两个性别中随机选择
                    sex_values = ["girl", "boy"]
                    row_values.append(f"'{random.choice(sex_values)}'")
                else:
                    # 使用纯ASCII字符生成随机字符串，避免UTF-8编码问题
                    import string
                    
                    # 根据数据类型确定字符串长度
                    if col.data_type.startswith("VARCHAR"):
                        # 尝试从VARCHAR定义中提取长度
                        match = re.search(r"VARCHAR\((\d+)\)", col.data_type)
                        if match:
                            max_length = int(match.group(1))
                            # 生成不超过最大长度的随机字符串
                            string_length = random.randint(1, min(max_length, 255))  # 防止过长
                        else:
                            # 默认VARCHAR长度
                            string_length = random.randint(5, 20)
                    elif col.data_type.startswith("CHAR"):
                        # 尝试从CHAR定义中提取长度
                        match = re.search(r"CHAR\((\d+)\)", col.data_type)
                        if match:
                            # CHAR类型使用固定长度
                            string_length = int(match.group(1))
                        else:
                            # 默认CHAR长度
                            string_length = 10
                    elif col.data_type == "TINYTEXT":
                        # TINYTEXT最大长度为255
                        string_length = random.randint(1, 200)
                    elif col.data_type == "TEXT":
                        # TEXT最大长度为65535
                        string_length = random.randint(1, 500)
                    elif col.data_type == "MEDIUMTEXT":
                        # MEDIUMTEXT最大长度为16777215
                        string_length = random.randint(1, 1000)
                    elif col.data_type == "LONGTEXT":
                        # LONGTEXT最大长度为4294967295
                        string_length = random.randint(1, 2000)
                    else:
                        # 默认长度
                        string_length = random.randint(5, 20)
                    
                    # 生成指定长度的随机字符串，确保sample_前缀算入总长度
                    prefix = 'sample_'
                    # 从总长度中减去前缀长度，确保总长度符合要求
                    random_part_length = max(1, string_length - len(prefix))  # 确保至少有1个随机字符
                    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=random_part_length))
                    row_values.append(f"'{prefix}{random_str}'")
            elif col.data_type == "DATE":
                # 生成随机日期值
                days = random.randint(0, 365)
                date_val = datetime.now() - timedelta(days=days)
                datetime_literal = dialect.get_datetime_literal(
                    date_val.year, date_val.month, date_val.day
                )
                row_values.append(datetime_literal)
            elif col.data_type == "TIME":
                # 生成随机时间值
                hours = random.randint(0, 23)
                minutes = random.randint(0, 59)
                seconds = random.randint(0, 59)
                datetime_literal = dialect.get_datetime_literal(
                    2023, 1, 1, hours, minutes, seconds
                )
                row_values.append(datetime_literal)
            elif col.data_type in ["DATETIME", "TIMESTAMP"]:
                # 生成随机日期时间值
                days = random.randint(0, 365)
                hours = random.randint(0, 23)
                minutes = random.randint(0, 59)
                seconds = random.randint(0, 59)
                date_val = datetime.now() - timedelta(days=days)
                datetime_literal = dialect.get_datetime_literal(
                    date_val.year, date_val.month, date_val.day, 
                    hours, minutes, seconds
                )
                row_values.append(datetime_literal)
            elif col.data_type == "BOOLEAN" or col.data_type == "BOOL":
                # 根据方言生成布尔值表示
                is_true = random.choice([True, False])
                if dialect.name == "POSTGRESQL":
                    # PostgreSQL使用TRUE/FALSE关键字
                    row_values.append("TRUE" if is_true else "FALSE")
                else:
                    # MySQL使用1/0或true/false字符串
                    row_values.append(str(is_true).lower())

            elif col.data_type.startswith("SET"):
                # SET类型：从允许的值中随机选择一个或多个
                # 解析SET定义中的可选值
                set_values = re.findall(r"'([^']+)',?", col.data_type)
                if set_values:
                    # 随机选择1到所有值的数量之间的元素
                    num_selected = random.randint(1, len(set_values))
                    selected_values = random.sample(set_values, num_selected)
                    row_values.append("'" + ",".join(selected_values) + "'")
                else:
                    row_values.append("''")
            elif col.data_type.startswith("ENUM"):
                # ENUM类型：从枚举值中随机选择一个
                enum_values = re.findall(r"'([^']+)',?", col.data_type)
                if enum_values:
                    row_values.append("'" + random.choice(enum_values) + "'")
                else:
                    row_values.append("NULL")
            elif col.data_type.startswith("BIT"):
                # BIT类型：生成随机位值
                # 解析位数
                bit_count = re.search(r"BIT\((\d+)\)", col.data_type)
                if bit_count:
                    bit_count = int(bit_count.group(1))
                    # 生成指定位数的随机二进制数
                    max_value = 2 ** bit_count - 1
                    row_values.append("b'" + bin(random.randint(0, max_value))[2:].zfill(bit_count) + "'")
                else:
                    row_values.append("b'0'")
            elif col.data_type in ["YEAR"]:
                # YEAR类型：生成2000-2023之间的随机年份
                row_values.append(str(random.randint(2000, 2023)))
            elif col.data_type in ["GEOMETRY", "POINT", "LINESTRING", "POLYGON"]:
                if get_current_dialect().name == "TIDB":
                    row_values.append("NULL")
                    continue
                # GEOMETRY类型：生成简单的几何数据
                if col.data_type == "POINT":
                    # 生成简单的点坐标
                    lat = round(random.uniform(-90, 90), 6)
                    lng = round(random.uniform(-180, 180), 6)
                    row_values.append(f"ST_GeomFromText('POINT({lat} {lng})')")
                elif col.data_type == "LINESTRING":
                    # 生成简单的线
                    points = []
                    for _ in range(2):
                        lat = round(random.uniform(-90, 90), 6)
                        lng = round(random.uniform(-180, 180), 6)
                        points.append(f"{lat} {lng}")
                    row_values.append(f"ST_GeomFromText('LINESTRING({','.join(points)})')")
                elif col.data_type == "POLYGON":
                    # 生成简单的多边形
                    points = []
                    for _ in range(4):
                        lat = round(random.uniform(-90, 90), 6)
                        lng = round(random.uniform(-180, 180), 6)
                        points.append(f"{lat} {lng}")
                    # 闭合多边形
                    points.append(points[0])
                    row_values.append(f"ST_GeomFromText('POLYGON(({','.join(points)}))')")
                else:
                    # 通用几何类型，默认为点
                    lat = round(random.uniform(-90, 90), 6)
                    lng = round(random.uniform(-180, 180), 6)
                    row_values.append(f"ST_GeomFromText('POINT({lat} {lng})')")
            elif col.data_type == "JSON" or col.data_type.startswith("JSON"):
                # JSON类型：生成随机JSON数据
                import json
                
                # 随机选择JSON结构类型
                json_types = ["simple_object", "nested_object", "array", "mixed"]
                json_type = random.choice(json_types)
                
                if json_type == "simple_object":
                    # 简单JSON对象
                    json_data = {
                        "k1": random.randint(1, 1000),
                        "k2": f"sample_{random.randint(1, 100)}",
                        "k3": random.uniform(10, 1000),
                        "k4": random.choice([True, False])
                    }
                elif json_type == "nested_object":
                    # 嵌套JSON对象
                    json_data = {
                        "k1": {
                            "k2": random.randint(1, 1000),
                            "k3": f"user_{random.randint(1, 100)}",
                            "k4": {
                                "k5": random.randint(18, 65),
                                "k6": f"user{random.randint(1, 100)}@example.com",
                                "k7": {
                                    "k8": f"city_{random.randint(1, 100)}",
                                    "k9": "Sample Country"
                                }
                            }
                        },
                        "k10": datetime.now().isoformat()
                    }
                elif json_type == "array":
                    # JSON数组
                    array_length = random.randint(1, 10)
                    json_data = [{
                        "k1": i,
                        "k2": f"item_{i}",
                        "k3": random.uniform(1, 100)
                    } for i in range(array_length)]
                else:
                    # 混合类型JSON
                    json_data = {
                        "k1": random.randint(1, 1000),
                        "k2": f"mixed_{random.randint(1, 100)}",
                        "k3": [f"tag_{random.randint(1, 100)}" for _ in range(random.randint(1, 5))],
                        "k4": {
                            "k5": random.choice(["A", "B", "C"]),
                            "k6": [random.randint(1, 100) for _ in range(random.randint(1, 5))],
                            "k7": {
                                "k8": f"user_{random.randint(1, 100)}",
                                "k9": datetime.now().isoformat()
                            }
                        },
                        "k10": random.choice([True, False])
                    }
                
                # 将JSON数据转换为字符串并添加到结果中
                json_str = json.dumps(json_data, ensure_ascii=False)
                row_values.append(f"'{json_str}'")
            elif col.data_type in ["BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB","BIT(8)"]:
                # BLOB类型：生成满足utf8mb4要求的二进制数据（用十六进制表示）
                # utf8mb4编码规则：
                # 1字节：0xxxxxxx
                # 2字节：110xxxxx 10xxxxxx
                # 3字节：1110xxxx 10xxxxxx 10xxxxxx
                # 4字节：11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                # 为确保兼容性，我们生成1-3字节的有效UTF-8序列
                utf8_bytes = bytearray()
                total_bytes = random.randint(1, 50)  # 严格限制总字节数不超过510，确保满足聚合函数要求
                current_size = 0
                
                while current_size < total_bytes:
                    # 随机选择生成1-3字节的UTF-8字符（覆盖大部分常用字符）
                    # 确保不会超出总字节数限制
                    remaining_bytes = total_bytes - current_size
                    if remaining_bytes >= 3:
                        byte_length = random.choice([1, 2, 3])
                    elif remaining_bytes >= 2:
                        byte_length = random.choice([1, 2])
                    else:
                        byte_length = 1
                    
                    if byte_length == 1:
                        # 1字节字符（ASCII）: 0xxxxxxx
                        utf8_bytes.append(random.randint(0x00, 0x7F))
                        current_size += 1
                    elif byte_length == 2:
                        # 2字节字符: 110xxxxx 10xxxxxx
                        utf8_bytes.append(random.randint(0xC0, 0xDF))  # 110xxxxx
                        utf8_bytes.append(random.randint(0x80, 0xBF))  # 10xxxxxx
                        current_size += 2
                    elif byte_length == 3:
                        # 3字节字符: 1110xxxx 10xxxxxx 10xxxxxx
                        utf8_bytes.append(random.randint(0xE0, 0xEF))  # 1110xxxx
                        utf8_bytes.append(random.randint(0x80, 0xBF))  # 10xxxxxx
                        utf8_bytes.append(random.randint(0x80, 0xBF))  # 10xxxxxx
                        current_size += 3
                
                # 转换为十六进制字符串
                hex_data = utf8_bytes.hex().upper()
                row_values.append(f"X'{hex_data}'")
            else:
                # 默认为 NULL
                row_values.append("NULL")

        # 构建单行 INSERT 语句
        values_str = ", ".join(row_values)
        insert_sql = f"INSERT INTO {table.name} ({', '.join(columns)}) VALUES ({values_str});"
        insert_sqls.append(insert_sql)

    # 将所有 INSERT 语句连接起来
    return "\n".join(insert_sqls)