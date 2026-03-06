from abc import ABC, abstractmethod


class DBDialect(ABC):
    """数据库方言抽象基类"""
    @property
    @abstractmethod
    def name(self):
        """数据库名称"""
        pass
    
    @abstractmethod
    def get_create_database_sql(self, db_name: str) -> str:
        """获取创建数据库的SQL语句"""
        pass
    
    @abstractmethod
    def get_use_database_sql(self, db_name: str) -> str:
        """获取使用数据库的SQL语句"""
        pass
    
    @abstractmethod
    def get_drop_database_sql(self, db_name: str) -> str:
        """获取删除数据库的SQL语句"""
        pass
    
    @abstractmethod
    def get_column_definition(self, col_name: str, data_type: str, nullable: bool, is_primary_key: bool = False) -> str:
        """获取列定义的SQL语句"""
        pass
    
    @abstractmethod
    def get_primary_key_constraint(self, primary_key: str) -> str:
        """获取主键约束的SQL语句"""
        pass
    
    @abstractmethod
    def get_datetime_literal(self, year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> str:
        """获取日期时间字面量的SQL表示"""
        pass
    
    @abstractmethod
    def get_function_name(self, function_name: str) -> str:
        """获取方言特定的函数名称"""
        pass
    
    @abstractmethod
    def get_literal_representation(self, value: str, data_type: str) -> str:
        """获取字面量的方言特定表示"""
        pass
    
    @abstractmethod
    def get_create_index_sql(self, table_name: str, index_name: str, columns: list, is_unique: bool = False) -> str:
        """获取创建MySQL索引的SQL语句"""
        pass
    
    @abstractmethod
    def get_session_settings_sql(self) -> str:
        """获取MySQL会话设置的SQL语句"""
        pass


class MySQLDialect(DBDialect):
    """MySQL数据库方言实现"""
    @property
    def name(self):
        return "MYSQL"
    
    def get_create_database_sql(self, db_name: str) -> str:
        return f"CREATE DATABASE IF NOT EXISTS {db_name};"
    
    def get_use_database_sql(self, db_name: str) -> str:
        return f"USE {db_name};"
    
    def get_drop_database_sql(self, db_name: str) -> str:
        return f"DROP DATABASE IF EXISTS {db_name};"
    
    def get_column_definition(self, col_name: str, data_type: str, nullable: bool, is_primary_key: bool = False) -> str:
        nullable_str = "NULL" if nullable else "NOT NULL"
        if is_primary_key and "INT" in data_type:
            return f"{col_name} {data_type} {nullable_str} AUTO_INCREMENT"
        return f"{col_name} {data_type} {nullable_str}"
    
    def get_primary_key_constraint(self, primary_key: str) -> str:
        return f"PRIMARY KEY ({primary_key})"
    
    def get_datetime_literal(self, year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> str:
        # 检查是否只需要日期部分（所有时间参数都是默认值0）
        if hour == 0 and minute == 0 and second == 0:
            return f"'{year:04d}-{month:02d}-{day:02d}'"
        else:
            return f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"
    
    def get_function_name(self, function_name: str) -> str:
        # MySQL函数名称映射
        function_mapping = {
            "TO_CHAR": "DATE_FORMAT",
            "VARIANCE_POP": "VAR_POP",
            "VARIANCE_SAMP": "VAR_SAMP"
        }
        return function_mapping.get(function_name, function_name)
    
    def get_literal_representation(self, value: str, data_type: str) -> str:
        # 确保data_type是字符串且不为None
        if data_type is None:
            data_type = 'UNKNOWN'
        
        # 特殊处理NULL值
        if value is None:
            return 'NULL'
        
        # 确保value是字符串
        value_str = str(value)
        
        # 特殊处理日期时间类型，确保总是添加引号
        if data_type.upper() in ['DATE', 'DATETIME', 'TIMESTAMP']:
            # 检查值是否已经被单引号或双引号包围
            if (value_str.startswith("'") and value_str.endswith("'") or 
                value_str.startswith('"') and value_str.endswith('"')):
                # 如果已经有引号，直接返回原始值（不进行额外转义）
                return value_str
            # 过滤非ASCII字符，确保只包含有效的UTF-8字符
            safe_value = ''.join(char for char in value_str if ord(char) < 128)
            # 转义单引号并添加引号
            escaped_value = safe_value.replace("'", "''")
            return f"'{escaped_value}'"
        elif data_type.upper() in ['VARCHAR', 'VARCHAR(255)', 'STRING']:
            # 确保value是字符串
            # 检查值是否已经被单引号或双引号包围
            if (value_str.startswith("'") and value_str.endswith("'") or 
                value_str.startswith('"') and value_str.endswith('"')):
                # 如果已经有引号，直接返回原始值（不进行额外转义）
                return value_str
            # 过滤非ASCII字符，确保只包含有效的UTF-8字符
            # 使用ascii编码，忽略非ASCII字符
            safe_value = ''.join(char for char in value_str if ord(char) < 128)
            # 转义单引号并添加引号
            escaped_value = safe_value.replace("'", "''")
            return f"'{escaped_value}'"
        elif data_type.upper() in ['BOOLEAN', 'BOOL']:
            # MySQL使用TRUE/FALSE字符串表示布尔值
            return f"'{str(value).lower()}'"
        elif data_type.upper() == 'JSON':
            # 确保JSON值使用正确的引号格式
            value_str = str(value)
            safe_value = ''.join(char for char in value_str if ord(char) < 128)
            escaped_value = safe_value.replace("'", "''")
            return f"'{escaped_value}'"
        elif data_type.upper() in ['BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB']:
            # 特殊处理二进制类型，确保满足utf8mb4要求
            # 检查值是否已经是十六进制格式（X'...'）
            if value_str.startswith("X'") and value_str.endswith("'"):
                # 提取十六进制部分
                hex_part = value_str[2:-1]
                # 确保十六进制数据长度为偶数（完整字节）
                if len(hex_part) % 2 != 0:
                    hex_part = '0' + hex_part
                # 验证十六进制字符
                if all(c in '0123456789ABCDEFabcdef' for c in hex_part):
                    # 对于utf8mb4，需要确保二进制数据是有效的UTF-8编码序列
                    # 由于我们无法直接在字符串表示中验证二进制数据的UTF-8有效性
                    # 我们选择使用MySQL的CONVERT函数将其明确转换为utf8mb4
                    return f"CONVERT({value_str} USING utf8mb4)"
            # 如果不是标准格式，返回空二进制值
            return "X''"
        # 对于其他数据类型，确保返回的字符串是有效的UTF-8
        value_str = str(value)
        # 检查是否为字符串类型的值
        if isinstance(value, str):
            # 过滤非ASCII字符，确保只包含有效的UTF-8字符
            safe_value = ''.join(char for char in value_str if ord(char) < 128)
            return safe_value
        return value_str
    
    def get_create_index_sql(self, table_name: str, index_name: str, columns: list, is_unique: bool = False) -> str:
        """获取创建MySQL索引的SQL语句"""
        unique_clause = "UNIQUE" if is_unique else ""
        columns_str = ", ".join(columns)
        return f"CREATE {unique_clause} INDEX {index_name} ON {table_name} ({columns_str});"
    
    def get_session_settings_sql(self) -> str:
        """获取MySQL会话设置的SQL语句"""
        return "SET GLOBAL sort_buffer_size = 64 * 1024 * 1024;\nSET GLOBAL read_rnd_buffer_size = 8 * 1024 * 1024;"


class PostgreSQLDialect(DBDialect):
    """PostgreSQL数据库方言实现"""
    @property
    def name(self):
        return "POSTGRESQL"
    
    def get_create_database_sql(self, db_name: str) -> str:
        # PostgreSQL不支持IF NOT EXISTS子句
        return f"CREATE DATABASE {db_name};"
    
    def get_use_database_sql(self, db_name: str) -> str:
        # PostgreSQL中没有USE语句，连接数据库通过连接参数指定
        return ""
    
    def get_drop_database_sql(self, db_name: str) -> str:
        # PostgreSQL删除数据库前需要断开所有连接
        terminate_sql = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}';"
        drop_sql = f"DROP DATABASE IF EXISTS {db_name};"
        return f"{terminate_sql}\n{drop_sql}"
    
    def get_column_definition(self, col_name: str, data_type: str, nullable: bool, is_primary_key: bool = False) -> str:
        nullable_str = "NULL" if nullable else "NOT NULL"
        if is_primary_key and "INT" in data_type:
            # PostgreSQL使用SERIAL或GENERATED ALWAYS AS IDENTITY
            return f"{col_name} SERIAL PRIMARY KEY"
        # PostgreSQL使用TIMESTAMP代替DATETIME
        if data_type == "DATETIME":
            data_type = "TIMESTAMP"
        return f"{col_name} {data_type} {nullable_str}"
    
    def get_primary_key_constraint(self, primary_key: str) -> str:
        # 对于SERIAL类型的主键，不需要单独添加PRIMARY KEY约束
        return f"PRIMARY KEY ({primary_key})"
    
    def get_datetime_literal(self, year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> str:
        # 检查是否只需要日期部分（所有时间参数都是默认值0）
        if hour == 0 and minute == 0 and second == 0:
            return f"'{year:04d}-{month:02d}-{day:02d}'"
        else:
            # PostgreSQL的日期时间格式与MySQL兼容
            return f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"
    
    def get_function_name(self, function_name: str) -> str:
        # PostgreSQL函数名称映射
        function_mapping = {
            "DATE_FORMAT": "TO_CHAR"
        }
        return function_mapping.get(function_name, function_name)
    
    def get_literal_representation(self, value: str, data_type: str) -> str:
        # 确保data_type是字符串且不为None
        if data_type is None:
            data_type = 'UNKNOWN'
        
        # 特殊处理NULL值
        if value is None:
            return 'NULL'
        
        # 确保value是字符串
        value_str = str(value)
        # 检查值是否已经被单引号或双引号包围
        is_quoted = (value_str.startswith("'") and value_str.endswith("'") or 
                    value_str.startswith('"') and value_str.endswith('"'))
        
        # 对于字符串、日期和时间戳类型，添加单引号
        if data_type.upper() in ['VARCHAR', 'VARCHAR(255)', 'TEXT', 'DATE', 'TIMESTAMP', 'STRING', 'DATETIME']:
            if is_quoted:
                # 如果已经有引号，直接返回原始值（不进行额外转义）
                return value_str
            # 否则转义单引号并添加引号
            escaped_value = value_str.replace("'", "''")
            # 确保返回带单引号的字符串
            return f"'{escaped_value}'"
        elif data_type.upper() in ['BOOLEAN', 'BOOL']:
            # PostgreSQL使用TRUE/FALSE关键字表示布尔值
            return str(value).upper()
        # 对于其他类型（如数值类型），直接返回字符串表示
        return str(value)
    
    def get_create_index_sql(self, table_name: str, index_name: str, columns: list, is_unique: bool = False) -> str:
        """获取创建PostgreSQL索引的SQL语句"""
        unique_clause = "UNIQUE" if is_unique else ""
        columns_str = ", ".join(columns)
        return f"CREATE {unique_clause} INDEX {index_name} ON {table_name} ({columns_str});"

    def get_session_settings_sql(self) -> str:
        """获取PostgreSQL会话设置的SQL语句"""
        return ""


class MariaDBDialect(MySQLDialect):
    """MariaDB数据库方言实现"""
    @property
    def name(self):
        return "MARIADB"
    
    def get_column_definition(self, col_name: str, data_type: str, nullable: bool, is_primary_key: bool = False) -> str:
        # MariaDB将JSON视为LONGTEXT的别名，需要特殊处理
        if data_type.upper() == "JSON":
            data_type = "LONGTEXT"
        # 对于GEOMETRY类型，保持原样但注意MariaDB的实现差异
        return super().get_column_definition(col_name, data_type, nullable, is_primary_key)
    
    def get_function_name(self, function_name: str) -> str:
        # MariaDB特定的函数名称映射
        function_mapping = {
            "TO_CHAR": "DATE_FORMAT",
            "VARIANCE_POP": "VAR_POP",
            "VARIANCE_SAMP": "VAR_SAMP"
            # 保留与MySQL相同的函数映射，但注意MariaDB可能有实现差异
        }
        return function_mapping.get(function_name, function_name)
    
    def get_literal_representation(self, value: str, data_type: str) -> str:
        # 确保data_type是字符串且不为None
        if data_type is None:
            data_type = 'UNKNOWN'
        
        # 特殊处理JSON类型：MariaDB需要将JSON作为字符串并使用双引号包裹
        if data_type.upper() == "JSON":
            import json
            # 确保value是字符串
            value_str = str(value)
            # 过滤非ASCII字符，确保只包含有效的UTF-8字符
            safe_value = ''.join(char for char in value_str if ord(char) < 128)
            # 转义双引号并使用双引号包裹
            escaped_value = safe_value.replace('"', '\\"')
            return f'"{escaped_value}"'
        
        # MariaDB将JSON视为LONGTEXT别名，按LONGTEXT处理
        if data_type.upper() == "JSON":
            data_type = "LONGTEXT"
        
        # 对于BOOLEAN类型，MariaDB内部使用TINYINT(1)表示
        if data_type.upper() in ['BOOLEAN', 'BOOL']:
            # 确保value是字符串
            value_str = str(value).lower() if value is not None else 'null'
            if value_str in ['true', 'false']:
                return '1' if value_str == 'true' else '0'
        
        # 对于空间数据类型，保持特殊处理但注意MariaDB的差异
        if data_type.upper() in ['GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON']:
            # MariaDB的空间数据处理可能与MySQL有细微差异，返回空值以避免问题
            return "NULL"
        
        # 调用父类方法处理其他数据类型
        return super().get_literal_representation(value, data_type)
    
    def supports_native_json(self) -> bool:
        """MariaDB不支持原生JSON类型，而是将其视为LONGTEXT别名"""
        return False
    
    def supports_math_equivalence_transformations(self) -> bool:
        """MariaDB的数学等价变换可能存在问题，特别是MIN/MAX与负数值转换"""
        return False


class TiDBDialect(MySQLDialect):
    """TiDB数据库方言实现"""
    @property
    def name(self):
        return "TIDB"
    
    def supports_subqueries_in_join_condition(self) -> bool:
        """TiDB不支持在ON条件中使用子查询"""
        return False
        
    def supports_share_lock_mode(self) -> bool:
        """TiDB对LOCK IN SHARE MODE只有空操作实现，应避免使用"""
        return False


class OceanBaseDialect(MySQLDialect):
    """OceanBase数据库方言实现"""
    @property
    def name(self):
        return "OCEANBASE"


class PerconaDialect(MySQLDialect):
    """Percona数据库方言实现"""
    @property
    def name(self):
        return "PERCONA"


class PolarDBDialect(MySQLDialect):
    """PolarDB数据库方言实现"""
    @property
    def name(self):
        return "POLARDB"
    
    def get_column_definition(self, col_name: str, data_type: str, nullable: bool, is_primary_key: str) -> str:
        # 对于外键列，不添加外键约束相关逻辑
        # 只处理基本的列定义和主键自增
        nullable_str = "NULL" if nullable else "NOT NULL"
        if is_primary_key and "INT" in data_type:
            return f"{col_name} {data_type} {nullable_str} AUTO_INCREMENT"
        return f"{col_name} {data_type} {nullable_str}"
    
    def get_primary_key_constraint(self, primary_key: str) -> str:
        # 正常处理主键约束
        return f"PRIMARY KEY ({primary_key})"
    
    def get_function_name(self, function_name: str) -> str:
        # PolarDB-X不支持的聚合函数映射
        # 将不支持的标准偏差函数和方差函数映射到AVG函数作为替代
        unsupported_functions = {
            "STD": "AVG",
            "STDDEV": "AVG",
            "STDDEV_POP": "AVG",
            "STDDEV_SAMP": "AVG",
            "VARIANCE": "AVG",
            "VARIANCE_POP": "AVG",
            "VARIANCE_SAMP": "AVG",
            "VAR_POP": "AVG",
            "VAR_SAMP": "AVG"
        }
        
        # 先检查是否是不支持的函数
        if function_name in unsupported_functions:
            return unsupported_functions[function_name]
        
        # 然后应用MySQL的函数映射
        function_mapping = {
            "TO_CHAR": "DATE_FORMAT"
        }
        return function_mapping.get(function_name, function_name)
    
    def supports_foreign_keys(self) -> bool:
        """PolarDB不支持外键"""
        return False
    
    def supports_subqueries_in_join(self) -> bool:
        """PolarDB不支持JOIN条件中的子查询"""
        return False
    
    def supports_share_lock_mode(self) -> bool:
        """PolarDB支持LOCK IN SHARE MODE，但对FOR KEY SHARE等其他模式需要适配"""
        return True
    
    def supports_except_operator(self) -> bool:
        """PolarDB-X是否支持EXCEPT操作符"""
        # PolarDB-X作为MySQL兼容数据库，默认不直接支持EXCEPT操作符
        return False
    
    def supports_intersect_operator(self) -> bool:
        """PolarDB-X是否支持INTERSECT操作符"""
        # PolarDB-X作为MySQL兼容数据库，默认不直接支持INTERSECT操作符
        return False


class DBDialectFactory:
    """数据库方言工厂类"""
    _dialects = {
        "MYSQL": MySQLDialect,
        "POSTGRESQL": PostgreSQLDialect,
        "MARIADB": MariaDBDialect,
        "TIDB": TiDBDialect,
        "OCEANBASE": OceanBaseDialect,
        "PERCONA": PerconaDialect,
        "POLARDB": PolarDBDialect
    }
    
    _current_dialect = None
    
    @classmethod
    def get_dialect(cls, dialect_name: str) -> DBDialect:
        """获取指定名称的数据库方言实例"""
        dialect_name = dialect_name.upper()
        if dialect_name not in cls._dialects:
            raise ValueError(f"不支持的数据库方言: {dialect_name}")
        return cls._dialects[dialect_name]()
    
    @classmethod
    def set_current_dialect(cls, dialect_name: str) -> None:
        """设置当前使用的数据库方言"""
        cls._current_dialect = cls.get_dialect(dialect_name)
    
    @classmethod
    def get_current_dialect(cls) -> DBDialect:
        """获取当前使用的数据库方言"""
        if cls._current_dialect is None:
            # 默认使用MySQL方言
            cls.set_current_dialect("MYSQL")
        return cls._current_dialect


# 设置默认方言为MySQL
def get_current_dialect() -> DBDialect:
    """获取当前使用的数据库方言的便捷函数"""
    return DBDialectFactory.get_current_dialect()

def set_current_dialect(dialect_name: str) -> None:
    """设置当前使用的数据库方言的便捷函数"""
    DBDialectFactory.set_current_dialect(dialect_name)

# 为了向后兼容，添加这些函数别名
def set_dialect(dialect_name: str) -> None:
    """设置当前使用的数据库方言（别名函数，兼容现有代码）"""
    DBDialectFactory.set_current_dialect(dialect_name)

def get_dialect_config() -> DBDialect:
    """获取当前使用的数据库方言配置（别名函数，兼容现有代码）"""
    return DBDialectFactory.get_current_dialect()
